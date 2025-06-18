from odoo import models, fields, api, tools, _
from odoo.exceptions import UserError, ValidationError
import logging
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from datetime import timedelta, datetime, time



load_dotenv()
_logger = logging.getLogger(__name__)


class ScreeningList(models.Model):
    _name = 'screening.list.metadata'
    _description = 'Screening List Metadata'

    name = fields.Char(string='List Name', required=True)
    list_type = fields.Selection([
        ('pep', 'PEP List'),
        ('watchlist', 'Watchlist'),
        ('sanction', 'Sanction List'),
        ('global_pep', 'Global PEP'),
        ('blacklist', 'Blacklist'),
        ('fep', 'FEP List')
    ], string='List Type', required=True)
    model_name = fields.Char(string='Model Name', required=True)
    last_updated = fields.Datetime(string='Last Updated', readonly=True)
    last_screening = fields.Datetime(string='Last Screening', readonly=True)
    record_count = fields.Integer(string='Record Count', readonly=True)

    def update_metadata(self):
        """Update list metadata"""
        for record in self:
            model = self.env[record.model_name]
            record_count = model.search_count([])
            record.write({
                'record_count': record_count,
                'last_updated': fields.Datetime.now()
            })
        return True


class CustomerScreeningResult(models.Model):
    _name = 'res.partner.screening.result'
    _description = 'Customer Screening Result'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'create_date desc'

    partner_id = fields.Many2one(
        'res.partner', string='Customer', required=True, index=True, ondelete='cascade', readonly=True)
    list_type = fields.Selection([
        ('pep', 'PEP List'),
        ('watchlist', 'Watchlist'),
        ('sanction', 'Sanction List'),
        ('global_pep', 'Global PEP'),
        ('blacklist', 'Blacklist'),
        ('fep', 'FEP List')
    ], string='List Type', required=True, index=True, readonly=True)
    match_id = fields.Reference(selection=[
        ('pep.list', 'PEP List'),
        ('res.partner.watchlist', 'Watchlist'),
        ('sanction.list', 'Sanction List'),
        ('res.pep', 'Global PEP'),
        ('res.partner.blacklist', 'Blacklist'),
        ('res.partner.fep', 'FEP List'),
    ], string='Matched Record', index=True, readonly=True)
    risk_score = fields.Float(
        related='partner_id.risk_score', string='Risk Score', readonly=True)
    risk_level = fields.Char(
        related='partner_id.risk_level', string='Risk Level', readonly=True)
    state = fields.Selection([
        ('pending', 'Pending Review'),
        ('confirmed', 'Confirmed Match'),
        ('dismissed', 'Dismissed')
    ], string='Status', default='pending', tracking=True, index=True)
    active = fields.Boolean(string='Active', default=True, index=True,
                            help="If the match is no longer valid, it will be set to inactive", readonly=True)
    reviewed_by_id = fields.Many2one('res.users', string='Reviewed By')
    review_date = fields.Datetime(string='Review Date', readonly=True)
    notes = fields.Text(string='Review Notes')
    screening_id = fields.Many2one(
        'res.partner.screening', string='Related Screening', required=True)


    
    def action_confirm(self):
        """Confirm the screening match"""
        for record in self:
            if record.state != 'pending':
                continue

            record.write({
                'state': 'confirmed',
                'reviewed_by_id': self.env.user.id,
                'review_date': fields.Datetime.now()
            })

            # Update the corresponding flag on customer
            if record.list_type == 'pep':
                record.partner_id.write({'is_pep': True})
            elif record.list_type == 'watchlist':
                record.partner_id.write({'is_watchlist': True})
            elif record.list_type == 'sanction':
                record.partner_id.write({'likely_sanction': True})
            elif record.list_type == 'global_pep':
                record.partner_id.write({'global_pep': True})
            elif record.list_type == 'blacklist':
                record.partner_id.write({'is_blacklist': True})
            elif record.list_type == 'fep':
                record.partner_id.write({'is_fep': True})

            # Recalculate risk score using existing method
            record.partner_id.action_compute_risk_score_with_plan()


        return True

    def action_dismiss(self):
        """Dismiss the screening match"""
        for record in self:
            if record.state != 'pending':
                continue

            record.write({
                'state': 'dismissed',
                'reviewed_by_id': self.env.user.id,
                'review_date': fields.Datetime.now()
            })


        return True
    
    
class CustomerScreening(models.Model):
    _name = 'res.partner.screening'
    _description = 'Customer Screening'

    @api.model
    def _normalize_name(self, name):
        """Normalize name for comparison by removing spaces and converting to lowercase"""
        if not name:
            return ""
        return name.lower().replace(" ", "").strip()

    @api.model
    def _screen_customer_against_pep(self, partner):
        """Screen customer against PEP list using optimized SQL query"""
        results = []

        # Skip if no name information
        if not partner.name and not partner.firstname and not partner.lastname:
            return results

        # Get normalized customer names
        customer_name = self._normalize_name(partner.name)
        customer_firstname = self._normalize_name(partner.firstname)
        customer_lastname = self._normalize_name(partner.lastname)

        if not customer_name and not customer_firstname and not customer_lastname:
            return results

        # Build SQL query for optimized matching
        query = """
            SELECT id, name, firstname, lastname
            FROM pep_list
            WHERE 1=0
        """
        params = []

        if customer_name:
            query = query.replace("1=0", "LOWER(REPLACE(name, ' ', '')) = %s")
            params.append(customer_name)

        if customer_firstname and customer_lastname:
            if params:
                query = query.replace(
                    "1=0", "1=0 OR (LOWER(REPLACE(firstname, ' ', '')) = %s AND LOWER(REPLACE(lastname, ' ', '')) = %s)")
                params.extend([customer_firstname, customer_lastname])
            else:
                query = query.replace(
                    "1=0", "(LOWER(REPLACE(firstname, ' ', '')) = %s AND LOWER(REPLACE(lastname, ' ', '')) = %s)")
                params.extend([customer_firstname, customer_lastname])

        if not params:
            return results

        self.env.cr.execute(query, tuple(params))
        pep_records = self.env.cr.dictfetchall()

        for pep in pep_records:
            results.append({
                'list_type': 'pep',
                'match_id': f'pep.list,{pep["id"]}'
            })

            # Update the likely match for quick reference
            partner.write({'likely_pep_match_id': pep["id"]})
            break

        return results

    @api.model
    def _screen_customer_against_watchlist(self, partner):
        """Screen customer against Watchlist using BVN with direct SQL query"""
        results = []
        
        # Skip if no BVN
        if not partner.bvn:
            _logger.info(f"Customer {partner.id} ({partner.name}) has no BVN, skipping watchlist screening")
            return results
        
        # Normalize BVN (remove whitespace and convert to string)
        normalized_bvn = str(partner.bvn).strip()
        _logger.info(f"Screening customer {partner.id} ({partner.name}) with BVN: '{normalized_bvn}'")
        
        # Try exact match first
        self.env.cr.execute("""
            SELECT id FROM res_partner_watchlist
            WHERE bvn = %s
            LIMIT 1
        """, (normalized_bvn,))
        
        watchlist_record = self.env.cr.fetchone()
        
        # If no exact match, try with trimmed values (to handle whitespace issues)
        if not watchlist_record:
            _logger.info(f"No exact match found for BVN: '{normalized_bvn}', trying with TRIM")
            self.env.cr.execute("""
                SELECT id FROM res_partner_watchlist
                WHERE TRIM(bvn) = %s
                LIMIT 1
            """, (normalized_bvn,))
            watchlist_record = self.env.cr.fetchone()
        
        # If still no match, log available BVNs in watchlist for debugging
        if not watchlist_record:
            _logger.info(f"No match found for BVN: '{normalized_bvn}', checking available BVNs in watchlist")
            self.env.cr.execute("""
                SELECT bvn FROM res_partner_watchlist
                LIMIT 10
            """)
            available_bvns = [r[0] for r in self.env.cr.fetchall()]
            _logger.info(f"Sample BVNs in watchlist: {available_bvns}")
        else:
            _logger.info(f"Found watchlist match for BVN: '{normalized_bvn}', record ID: {watchlist_record[0]}")
            results.append({
                'list_type': 'watchlist',
                'match_id': f'res.partner.watchlist,{watchlist_record[0]}'
            })
            
            # Update the likely match for quick reference
            partner.write({'likely_watchlist_match_id': watchlist_record[0]})
                
        return results

    @api.model
    def _screen_customer_against_sanction(self, partner):
        """Screen customer against Sanction list using optimized SQL query"""
        results = []

        # Skip if no name information
        if not partner.name and not partner.firstname and not partner.lastname:
            return results

        # Get normalized customer names
        customer_name = self._normalize_name(partner.name)
        customer_firstname = self._normalize_name(partner.firstname)
        customer_lastname = self._normalize_name(partner.lastname)

        if not customer_name and not customer_firstname and not customer_lastname:
            return results

        # Build SQL query for optimized matching
        query = """
            SELECT id, name, first_name, surname
            FROM sanction_list
            WHERE 1=0
        """
        params = []

        if customer_name:
            query = query.replace("1=0", "LOWER(REPLACE(name, ' ', '')) = %s")
            params.append(customer_name)

        if customer_firstname and customer_lastname:
            if params:
                query = query.replace(
                    "1=0", "1=0 OR (LOWER(REPLACE(first_name, ' ', '')) = %s AND LOWER(REPLACE(surname, ' ', '')) = %s)")
                params.extend([customer_firstname, customer_lastname])
            else:
                query = query.replace(
                    "1=0", "(LOWER(REPLACE(first_name, ' ', '')) = %s AND LOWER(REPLACE(surname, ' ', '')) = %s)")
                params.extend([customer_firstname, customer_lastname])

        if not params:
            return results

        self.env.cr.execute(query, tuple(params))
        sanction_records = self.env.cr.dictfetchall()

        for sanction in sanction_records:
            results.append({
                'list_type': 'sanction',
                'match_id': f'sanction.list,{sanction["id"]}'
            })

            # Update the likely match for quick reference
            partner.write({'likely_sanction_match_id': sanction["id"]})
            break

        return results

    @api.model
    def _screen_customer_against_global_pep(self, partner):
        """Screen customer against Global PEP list using optimized SQL query"""
        results = []

        # Skip if no name information
        if not partner.name and not partner.firstname and not partner.lastname:
            return results

        # Get normalized customer names
        customer_name = self._normalize_name(partner.name)
        customer_firstname = self._normalize_name(partner.firstname)
        customer_lastname = self._normalize_name(partner.lastname)

        if not customer_name and not customer_firstname and not customer_lastname:
            return results

        # Build SQL query for optimized matching
        query = """
            SELECT id, name, first_name, surname
            FROM res_pep
            WHERE 1=0
        """
        params = []

        if customer_name:
            query = query.replace("1=0", "LOWER(REPLACE(name, ' ', '')) = %s")
            params.append(customer_name)

        if customer_firstname and customer_lastname:
            if params:
                query = query.replace(
                    "1=0", "1=0 OR (LOWER(REPLACE(first_name, ' ', '')) = %s AND LOWER(REPLACE(surname, ' ', '')) = %s)")
                params.extend([customer_firstname, customer_lastname])
            else:
                query = query.replace(
                    "1=0", "(LOWER(REPLACE(first_name, ' ', '')) = %s AND LOWER(REPLACE(surname, ' ', '')) = %s)")
                params.extend([customer_firstname, customer_lastname])

        if not params:
            return results

        self.env.cr.execute(query, tuple(params))
        pep_records = self.env.cr.dictfetchall()

        for pep in pep_records:
            results.append({
                'list_type': 'global_pep',
                'match_id': f'res.pep,{pep["id"]}'
            })

            # Update the likely match for quick reference
            partner.write({'likely_global_pep_match_id': pep["id"]})
            break

        return results


    @api.model
    def screen_customer(self, partner_id):
        """Screen a customer against all lists"""
        partner = self.env['res.partner'].browse(partner_id)
        if not partner.exists():
            return False
            
        # Get or create screening status
        status = self.env['res.partner.screening.status'].get_status(partner_id)
        
        all_results = []
        
        # Get changed lists
        changed_lists = self.check_list_changes()
        
        # Only screen against changed lists
        if 'pep' in changed_lists or not status.pep_status:
            all_results.extend(self._screen_customer_against_pep(partner))
            status.pep_status = True
            
        if 'watchlist' in changed_lists or not status.watchlist_status:
            all_results.extend(self._screen_customer_against_watchlist(partner))
            status.watchlist_status = True
            
        if 'sanction' in changed_lists or not status.sanction_status:
            all_results.extend(self._screen_customer_against_sanction(partner))
            status.sanction_status = True
            
        if 'global_pep' in changed_lists or not status.global_pep_status:
            all_results.extend(self._screen_customer_against_global_pep(partner))
            status.global_pep_status = True
        
        # ... handle other lists similarly ...
        
        # Create screening results
        for result in all_results:
            # Check if a similar result already exists
            existing = self.env['res.partner.screening.result'].search([
                ('partner_id', '=', partner.id),
                ('list_type', '=', result['list_type']),
                ('active', '=', True)
            ], limit=1)
            
            if existing:
                # Update existing result if the match_id is different
                if existing.match_id != result['match_id']:
                    existing.write({
                        'match_id': result['match_id'],
                        'state': 'pending',  # Reset to pending since it's a new match
                        'reviewed_by_id': False,
                        'review_date': False
                    })
            else:
                # Create new result
                self.env['res.partner.screening.result'].create({
                    'partner_id': partner.id,
                    'list_type': result['list_type'],
                    'match_id': result['match_id'],
                    'state': 'pending',
                    'active': True
                })
        
        # Update last screening date
        status.write({'last_screening_date': fields.Datetime.now()})
        
        # If matches found, update risk score
        if all_results:
            partner.action_compute_risk_score_with_plan()
            self._send_screening_notification(partner, all_results)
            
        return bool(all_results)
    
    # @api.model
    # def _send_screening_notification(self, partner, results):
    #     """Send email notification for screening results"""
        
    #     sc_alert_records = self.env['res.partner.screening.alert'].search([])
    #     # Collect all users from the alert_officers Many2many field
    #     officers = sc_alert_records.mapped('sanction_alert_officers')
    #     if not officers:
    #         _logger.warning("No officers configured for alerts")
    #         return
        
    #     # from_filter = self.env['ir.config_parameter'].sudo().get_param(
    #     #     'mail.default.from', tools.config.get('from_filter'))
    #     from_filter = self.env['ir.config_parameter'].sudo().get_param(
    #         'mail.default.from_filter', tools.config.get('from_filter'))
        
    #     if not from_filter:
    #         _logger.error("EmailFrom variable not configured in ir.config_parameter settings")
    #         raise ValidationError("Email sender address not configured")
        
    #     # officers_name = ", ".join(officers.mapped('name')) or ""
    #     officers_email = ", ".join(officers.mapped('email')) or ""
                

    #     template = self.env.ref(
    #         'compliance_management.email_template_screening_alert', raise_if_not_found=False)
    #     if not template:
    #         _logger.warning("Email template for screening alert not found")
    #         return


    #     # Prepare context for template
    #     ctx = {
    #         'partner': partner,
    #         'results': results,
    #         'base_url': self.env['ir.config_parameter'].sudo().get_param('web.base.url')
    #     }

    #     # Send email to each compliance officer
    #     for officer in officers:
    #         template.with_context(ctx).send_mail(
    #             partner.id,
    #             force_send=True,
    #             email_values={'email_to': officer.email}
    #         )

    @api.model
    def _send_screening_notification(self, partner, results):
        """Send email notification for screening results"""
        try:
            sc_alert_records = self.env['res.partner.screening.alert'].search([])
            # Collect all users from the alert_officers Many2many field
            officers = sc_alert_records.mapped('sanction_alert_officers')
            if not officers:
                _logger.warning("No officers configured for alerts")
                return

            from_filter = self.env['ir.config_parameter'].sudo().get_param(
                'mail.default.from_filter', tools.config.get('from_filter'))

            if not from_filter:
                _logger.error(
                    "EmailFrom variable not configured in ir.config_parameter settings")
                raise ValidationError("Email sender address not configured")

            officers_email = ", ".join(officers.mapped('email')) or ""

            template = self.env.ref(
                'compliance_management.email_template_screening_alert', raise_if_not_found=False)
            if not template:
                _logger.warning("Email template for screening alert not found")
                return

            # Prepare context for template
            ctx = {
                'partner': partner,
                'results': results,
                'base_url': self.env['ir.config_parameter'].sudo().get_param('web.base.url'),
                'officers_email': officers_email,
                'datetime': datetime.now().replace(microsecond=0)
            }

            # Render the template to get HTML content for alert history
            template_with_ctx = template.with_context(**ctx)

            # Get the rendered HTML content
            rendered_html = template_with_ctx._render_template(
                template_with_ctx.body_html,
                template_with_ctx.model,
                [partner.id],
                engine='qweb',
                add_context=ctx
            )[partner.id]

            # Determine risk level based on partner's risk score
            partner_risk_score = float(
                partner.risk_score) if partner.risk_score else 0.0
            alert_risk_level = "low"  # default

            try:
                low_threshold = float(
                    self.env['res.compliance.settings'].get_setting('low_risk_threshold'))
                medium_threshold = float(
                    self.env['res.compliance.settings'].get_setting('medium_risk_threshold'))

                if partner_risk_score <= low_threshold:
                    alert_risk_level = "low"
                elif partner_risk_score <= medium_threshold:
                    alert_risk_level = "medium"
                else:
                    alert_risk_level = "high"
            except Exception as e:
                _logger.warning(
                    f"Could not determine risk thresholds, using default 'low': {str(e)}")

            # Send email to all compliance officers at once
            try:
                email_result = template_with_ctx.send_mail(
                    partner.id,
                    force_send=True,
                    raise_exception=True,
                    email_values={
                        'email_to': officers_email,
                        'email_from': from_filter
                    }
                )

                # Check if email was sent successfully
                mail = self.env['mail.mail'].browse(email_result)
                email_sent_successfully = mail.state == 'sent'

                if email_sent_successfully:
                    _logger.info(
                        f"Screening notification sent to officers: {officers_email}")

            except Exception as e:
                _logger.error(f"Failed to send screening notification: {str(e)}")
                email_sent_successfully = False

            # Insert into alert history table if at least one email was sent successfully
            if email_sent_successfully:
                try:
                    self.env['alert.history'].sudo(flag=True).create({
                        "ref_id": f"res.partner.screening.result,{partner.id}",
                        'html_body': rendered_html,
                        'attachment_data': None,
                        'attachment_link': None,
                        'last_checked': fields.Datetime.now(),
                        'risk_rating': alert_risk_level,
                        'process_id': None,
                        'source': 'Sanction Screening Alert',
                        'date_created': fields.Datetime.now(),
                        'email': officers_email
                    })
                    _logger.info(
                        f"Screening alert history record created for partner: {partner.name}")

                except Exception as e:
                    _logger.error(
                        f"Failed to create alert history record: {str(e)}")

        except Exception as e:
            _logger.error(
                f"Error in screening notification process: {str(e)}", exc_info=True)
            raise ValidationError(
                f"Failed to send screening notifications: {str(e)}")
    
    @api.model
    def _get_list_details(self):
        """Get details of all screening lists"""
        return [
            {'type': 'pep', 'model': 'pep.list',
                'name': 'PEP List', 'field': 'is_pep'},
            {'type': 'watchlist', 'model': 'res.partner.watchlist',
                'name': 'Watchlist', 'field': 'is_watchlist'},
            {'type': 'sanction', 'model': 'sanction.list',
                'name': 'Sanction List', 'field': 'likely_sanction'},
            {'type': 'global_pep', 'model': 'res.pep',
                'name': 'Global PEP List', 'field': 'global_pep'},
            # For blacklist and fep, you'll need to specify the correct models
            {'type': 'blacklist', 'model': 'res.partner.blacklist',
                'name': 'Blacklist', 'field': 'is_blacklist'},
            {'type': 'fep', 'model': 'res.partner.fep',
                'name': 'FEP List', 'field': 'is_fep'}
        ]

    @api.model
    def _update_list_metadata(self):
        """Update metadata for all screening lists"""
        list_details = self._get_list_details()

        for list_detail in list_details:
            try:
                # Skip if model doesn't exist
                if not self.env['ir.model'].search([('model', '=', list_detail['model'])]):
                    continue

                # Get or create metadata record
                metadata = self.env['screening.list.metadata'].search([
                    ('list_type', '=', list_detail['type']),
                    ('model_name', '=', list_detail['model'])
                ], limit=1)

                if not metadata:
                    metadata = self.env['screening.list.metadata'].create({
                        'name': list_detail['name'],
                        'list_type': list_detail['type'],
                        'model_name': list_detail['model']
                    })

                # Update record count
                model = self.env[list_detail['model']]
                record_count = model.search_count([])

                metadata.write({
                    'record_count': record_count,
                    'last_updated': fields.Datetime.now()
                })
            except Exception as e:
                _logger.error(
                    f"Error updating metadata for {list_detail['name']}: {e}")
                continue

        return True

    @api.model
    def check_list_changes(self):
        """Check if any lists have changed since last screening"""
        self._update_list_metadata()
        changed_lists = []

        list_metadata = self.env['screening.list.metadata'].search([])
        for metadata in list_metadata:
            # Skip if no last screening date (never screened)
            if not metadata.last_screening:
                changed_lists.append(metadata.list_type)
                continue

            # Skip if no last updated date
            if not metadata.last_updated:
                continue

            # Check if list was updated after last screening
            if metadata.last_updated > metadata.last_screening:
                changed_lists.append(metadata.list_type)

        return changed_lists

    @api.model
    def _deactivate_outdated_matches(self, list_type):
        """Deactivate matches that no longer exist in the list"""
        list_details = next(
            (l for l in self._get_list_details() if l['type'] == list_type), None)
        if not list_details:
            return False

        # Get all current active matches for this list
        screening_results = self.env['res.partner.screening.result'].search([
            ('list_type', '=', list_type),
            ('active', '=', True),
            ('state', '=', 'confirmed')  # Only check confirmed matches
        ])

        deactivated_count = 0

        for result in screening_results:
            # Skip if no match_id
            if not result.match_id:
                continue

            # Check if the matched record still exists
            if not result.match_id.exists():
                # Record no longer exists, deactivate the match
                result.write({'active': False})

                # Reset the flag on the customer
                if list_type == 'pep':
                    result.partner_id.write({'is_pep': False})
                elif list_type == 'watchlist':
                    result.partner_id.write({'is_watchlist': False})
                elif list_type == 'sanction':
                    result.partner_id.write({'likely_sanction': False})
                elif list_type == 'global_pep':
                    result.partner_id.write({'global_pep': False})
                elif list_type == 'blacklist':
                    result.partner_id.write({'is_blacklist': False})
                elif list_type == 'fep':
                    result.partner_id.write({'is_fep': False})

                # Recompute risk score
                result.partner_id.action_compute_risk_score_with_plan()

                deactivated_count += 1

        _logger.info(
            f"Deactivated {deactivated_count} outdated matches for {list_type}")
        return deactivated_count

    # @api.model
    # def _cron_daily_screening(self, limit=1000):
    #     """Daily screening based on list changes"""
    #     # Check which lists have changed
    #     changed_lists = self.check_list_changes()

    #     if not changed_lists:
    #         _logger.info("No list changes detected, skipping screening")
    #         return {'screened': 0, 'matches': 0}

    #     _logger.info(f"Detected changes in lists: {', '.join(changed_lists)}")

    #     # Deactivate outdated matches for changed lists
    #     for list_type in changed_lists:
    #         self._deactivate_outdated_matches(list_type)

    #     # Get all customers to screen
    #     # Limit batch size for performance
    #     customers = self.env['res.partner'].search([], limit=limit)

    #     processed_count = 0
    #     match_count = 0

    #     for customer in customers:
    #         try:
    #             result = self.screen_customer(customer.id)
    #             processed_count += 1
    #             if result:
    #                 match_count += 1
    #         except Exception as e:
    #             _logger.error(f"Error screening customer {customer.id}: {e}")
    #             continue

    #     # Update last screening date for all lists
    #     now = fields.Datetime.now()
    #     list_metadata = self.env['screening.list.metadata'].search([
    #         ('list_type', 'in', changed_lists)
    #     ])
    #     list_metadata.write({'last_screening': now})

    #     _logger.info(
    #         f"Daily screening completed: {processed_count} processed, {match_count} matches found")

    #     return {
    #         'processed': processed_count,
    #         'matches': match_count
    #     }

    @api.model
    def _cron_daily_screening(self, limit=5000):
        """Daily screening based on list changes"""
        # Check which lists have changed
        changed_lists = self.check_list_changes()

        if not changed_lists:
            _logger.info("No list changes detected, skipping screening")
            return {'screened': 0, 'matches': 0}

        _logger.info(f"Detected changes in lists: {', '.join(changed_lists)}")

        # Deactivate outdated matches for changed lists
        for list_type in changed_lists:
            self._deactivate_outdated_matches(list_type)

        # Get customers that need screening
        # Either they've never been screened or their last screening was before the list was updated
        customers_to_screen = []

        # Use direct SQL for better performance
        self.env.cr.execute("""
            SELECT rp.id 
            FROM res_partner rp
            LEFT JOIN res_partner_screening_status rpss ON rp.id = rpss.partner_id
            WHERE rpss.id IS NULL OR rpss.last_screening_date IS NULL
            LIMIT %s
        """, (limit,))

        customers_to_screen.extend([r[0] for r in self.env.cr.fetchall()])

        # For each changed list, get customers that haven't been screened for that list
        status_fields = {
            'pep': 'pep_status',
            'watchlist': 'watchlist_status',
            'sanction': 'sanction_status',
            'global_pep': 'global_pep_status',
            'blacklist': 'blacklist_status',
            'fep': 'fep_status'
        }

        for list_type in changed_lists:
            if list_type in status_fields:
                self.env.cr.execute("""
                    SELECT rp.id
                    FROM res_partner rp
                    JOIN res_partner_screening_status rpss ON rp.id = rpss.partner_id
                    WHERE rpss.%s = FALSE
                    LIMIT %s
                """ % (status_fields[list_type], '%s'), (limit,))

                customers_to_screen.extend([r[0] for r in self.env.cr.fetchall()])

        # Remove duplicates
        customers_to_screen = list(set(customers_to_screen))

        # Process customers in smaller batches
        processed_count = 0
        match_count = 0

        batch_size = 100
        for i in range(0, len(customers_to_screen), batch_size):
            batch = customers_to_screen[i:i+batch_size]
            for customer_id in batch:
                try:
                    result = self.screen_customer(customer_id)
                    processed_count += 1
                    if result:
                        match_count += 1
                except Exception as e:
                    _logger.error(f"Error screening customer {customer_id}: {e}")
                    continue

            self.env.cr.commit()  # Commit after each batch
            _logger.info(
                f"Daily screening progress: {processed_count}/{len(customers_to_screen)}")

        # Update last screening date for all lists
        now = fields.Datetime.now()
        list_metadata = self.env['screening.list.metadata'].search([
            ('list_type', 'in', changed_lists)
        ])
        list_metadata.write({'last_screening': now})

        _logger.info(
            f"Daily screening completed: {processed_count} processed, {match_count} matches found")

        return {
            'processed': processed_count,
            'matches': match_count
        }
    
    @api.model
    def bulk_screen_customers(self, list_types=None, batch_size=1000):
        """API method to trigger batch screening for specific lists"""
        if list_types is None:
            list_types = [l['type'] for l in self._get_list_details()]

        # Deactivate outdated matches for specified lists
        for list_type in list_types:
            self._deactivate_outdated_matches(list_type)

        total_customers = self.env['res.partner'].search_count([])
        processed = 0
        matches = 0

        while processed < total_customers:
            customers = self.env['res.partner'].search(
                [], limit=batch_size, offset=processed)
            if not customers:
                break

            # Process in smaller chunks for better error handling
            chunk_size = 100
            for i in range(0, len(customers.ids), chunk_size):
                chunk = customers.ids[i:i+chunk_size]
                for customer_id in chunk:
                    try:
                        result = self.screen_customer(customer_id)
                        if result:
                            matches += 1
                    except Exception as e:
                        _logger.error(
                            f"Error in bulk screening for customer {customer_id}: {e}")

            self.env.cr.commit()  # Commit after each batch
            processed += len(customers)
            _logger.info(
                f"Bulk screening progress: {processed}/{total_customers}")

        # Update last screening date for all lists
        now = fields.Datetime.now()
        list_metadata = self.env['screening.list.metadata'].search([
            ('list_type', 'in', list_types)
        ])
        list_metadata.write({'last_screening': now})

        return {
            'total': total_customers,
            'processed': processed,
            'matches': matches
        }
        
    def init(self):
        # Add performance-critical indexes for large datasets
        self.env.cr.execute("""            
            CREATE INDEX IF NOT EXISTS res_partner_screening_result_active_idx 
            ON res_partner_screening_result (active, state, list_type);
            
            CREATE INDEX IF NOT EXISTS res_partner_screening_result_partner_active_idx 
            ON res_partner_screening_result (partner_id, active, state);
        """)
        
    def copy_screening_chatter_to_result(self):
        """Copy all messages and attachments from screening to screening result"""
        try:
            # Search for existing screening results for this screening record
            existing_results = self.env['res.partner.screening.result'].search([
                ('screening_id', '=', self.id)  # Assuming there's a relation field
            ], order='create_date desc', limit=1)

            # Prepare values for the new screening result
            result_vals = self._prepare_screening_result_vals()
            result_vals['screening_id'] = self.id

            # Create new screening result if none exists
            if not existing_results:
                try:
                    screening_result = self.env['res.partner.screening.result'].create(
                        result_vals)
                    _logger.info(
                        f"Created screening result record for screening {self.id}")
                except Exception as e:
                    _logger.error(f"Error creating screening result: {e}")
                    return None
            else:
                # Use existing screening result
                screening_result = existing_results[0]

            # Ensure screening_result is not None
            if not screening_result:
                _logger.error(
                    f"Failed to find or create screening result for screening {self.id}")
                return None

            # Copy all messages from screening to screening result
            messages = self.env['mail.message'].search([
                ('res_id', '=', self.id),
                ('model', '=', 'res.partner.screening')
            ], order='create_date asc')

            for message in messages:
                # Copy the message to the screening result
                try:
                    message.copy({
                        'model': 'res.partner.screening.result',
                        'res_id': screening_result.id
                    })
                    _logger.debug(
                        f"Copied message {message.id} to screening result {screening_result.id}")
                except Exception as e:
                    _logger.error(f"Error copying message {message.id}: {e}")
                    continue

            # Copy email templates and attachments
            if hasattr(self, 'message_main_attachment_id') and self.message_main_attachment_id:
                try:
                    # Link the main attachment to the screening result
                    self.message_main_attachment_id.copy({
                        'res_model': 'res.partner.screening.result',
                        'res_id': screening_result.id
                    })
                    _logger.debug(
                        f"Copied main attachment to screening result {screening_result.id}")
                except Exception as e:
                    _logger.error(f"Error copying main attachment: {e}")

            # Copy all other attachments related to this screening
            attachments = self.env['ir.attachment'].search([
                ('res_model', '=', 'res.partner.screening'),
                ('res_id', '=', self.id)
            ])

            for attachment in attachments:
                try:
                    attachment.copy({
                        'res_model': 'res.partner.screening.result',
                        'res_id': screening_result.id
                    })
                    _logger.debug(
                        f"Copied attachment {attachment.id} to screening result {screening_result.id}")
                except Exception as e:
                    _logger.error(f"Error copying attachment {attachment.id}: {e}")
                    continue

            _logger.info(
                f"Successfully copied {len(messages)} messages and {len(attachments)} attachments to screening result {screening_result.id}")
            return screening_result

        except Exception as e:
            _logger.error(
                f"Error in copy_screening_chatter_to_result: {e}", exc_info=True)
            return None


    def _prepare_screening_result_vals(self):
        """Prepare values for creating screening result record"""
        return {
            'partner_id': self.partner_id.id if hasattr(self, 'partner_id') else False,
            'screening_date': fields.Datetime.now(),
            'status': 'pending',
            'result_summary': f"Screening initiated for {self.partner_id.name if hasattr(self, 'partner_id') else 'Partner'}",
            # Add other fields as needed based on your model structure
        }


    @api.model_create_multi
    def create(self, vals_list):
        """Override create method to automatically copy messages when screening is created"""
        records = super().create(vals_list)

        # Copy messages for each created record
        for record in records:
            try:
                record.copy_screening_chatter_to_result()
            except Exception as e:
                _logger.error(
                    f"Error copying messages for screening {record.id}: {e}")

        return records
    
class CustomerScreeningStatus(models.Model):
    _name = 'res.partner.screening.status'
    _description = 'Customer Screening Status'

    partner_id = fields.Many2one('res.partner', string='Customer', required=True,
                                 index=True, ondelete='cascade')
    last_screening_date = fields.Datetime(
        string='Last Screening Date', index=True)
    pep_status = fields.Boolean(string='PEP Checked', default=False)
    watchlist_status = fields.Boolean(
        string='Watchlist Checked', default=False)
    sanction_status = fields.Boolean(string='Sanction Checked', default=False)
    global_pep_status = fields.Boolean(
        string='Global PEP Checked', default=False)
    blacklist_status = fields.Boolean(
        string='Blacklist Checked', default=False)
    fep_status = fields.Boolean(string='FEP Checked', default=False)

    # This ensures only one record per partner
    _sql_constraints = [
        ('unique_partner', 'unique(partner_id)',
         'Only one screening status per customer is allowed')
    ]

    @api.model
    def get_status(self, partner_id):
        """Get or create screening status for a partner"""
        status = self.search([('partner_id', '=', partner_id)], limit=1)
        if not status:
            status = self.create({'partner_id': partner_id})
        return status


class CustomerScreeningAlert(models.Model):
    _name = 'res.partner.screening.alert'
    _description = 'Sancation Screening Alert'
    _order = 'id desc'
    _rec_name = "create_date"

    _inherit = ['mail.thread', 'mail.activity.mixin']

    sanction_alert_officers = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        'sanction_screening_alert_officers',
        string="Officer(s) Responsible",
        tracking=True,
    )
    active = fields.Boolean(default=True, tracking=True)
