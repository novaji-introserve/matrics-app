from odoo import models, fields, api
from odoo.exceptions import AccessError
from dateutil.relativedelta import relativedelta
from odoo.exceptions import UserError, ValidationError
from datetime import timedelta, datetime, time
import re
from dotenv import load_dotenv
import os
import json
from concurrent.futures import ThreadPoolExecutor
import hashlib
import requests
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
from odoo.modules.module import get_module_resource
import base64
import logging
from .customer import LOW_RISK_THRESHOLD, MEDIUM_RISK_THRESHOLD, HIGH_RISK_THRESHOLD


load_dotenv()
_logger = logging.getLogger(__name__)


class AdverseMedia(models.Model):
    _name = 'adverse.media'
    _description = 'Adverse Media Screening'
    _order = "id desc"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    partner_id = fields.Many2one(
        'res.partner', string='Partner', tracking=True, required=True, index=True, ondelete='cascade',domain="[('origin', 'in', ['demo', 'test', 'prod'])]")
    partner_risk_score = fields.Float(
        related='partner_id.risk_score', tracking=True, string="Risk Score")
    partner_risk_level = fields.Char(
        related='partner_id.risk_level', tracking=True, string="Risk Level")

    monitoring_status = fields.Selection([
        ('active', 'Active'),
        ('inactive', 'Inactive'),
    ], string='Monitoring Status', default='active', tracking=True)

    monitoring_frequency = fields.Selection([
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        # ('yearly', 'Yearly'),
    ], string='Monitoring Frequency', default='daily', tracking=True)

    last_scan_date = fields.Datetime(string='Last Scan', tracking=True,)
    next_scan_date = fields.Datetime(
        string='Next Scan Date', tracking=True, compute='_compute_next_scan_date', store=True)

    keyword_id = fields.Many2many(
        'media.keyword', string='Keywords for Monitoring', required=True,
        tracking=True, index=True,        default=lambda self: self._default_keywords())

    alert_ids = fields.One2many(
        'adverse.media.alert', 'media_id', string='Alerts', index=True)

    risk_score_decoration = fields.Char(
        compute='_compute_risk_score_decoration')

    def _default_keywords(self):
        """Return all records from media.keyword as default value."""
        return self.env['media.keyword'].search([]).ids

    @api.depends('partner_risk_score')
    def _compute_risk_score_decoration(self):
        for record in self:
            if record.partner_risk_score <= 10:  # Match with 'low'
                record.risk_score_decoration = 'success'
            elif 10 < record.partner_risk_score <= 19:  # Match with 'medium'
                record.risk_score_decoration = 'warning'
            else:  # Match with 'high'
                record.risk_score_decoration = 'danger'

    def open_media_log(self):
        try:
            # Define your action here
            action = self.env.ref(
                "compliance_management.action_adverse_media_alert").sudo().read()[0]
            # Set the default domain to show tickets with matching issue
            id = self.id
            action["domain"] = [("media_id", "=", id)]
            return action
        except AccessError:
            # If the user lacks permissions, raise a friendly message
            raise AccessError(
                "You do not have the necessary permissions to view the Media Log.")

    def _prepare_search_query(self):
        """Prepare search query for NewsAPI"""
        try:
            partner_name = self.partner_id.name
            if not partner_name:
                _logger.error(f"Missing partner name for record ID: {self.id}")
                raise ValidationError(
                    "Partner name is required for media screening")

            keywords = self.keyword_id.mapped('name')
            _logger.critical(f"keywords to be used {keywords}")
            if not keywords:
                _logger.error(
                    f"No keywords configured for record ID: {self.id}")
                raise ValidationError(
                    "At least one keyword is required for media screening")

            # query_parts = [partner_name] + keywords
            query = [partner_name]
            # query = ' OR '.join(f'"{term}"' for term in query_parts)
            _logger.info(
                f"Generated search query for partner {partner_name}: {query}")
            return query

        except Exception as e:
            _logger.error(
                f"Error preparing search query: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to prepare search query: {str(e)}")

    def _fetch_news_articles(self):
        """Fetch news articles from NewsAPI with enhanced error handling"""
        api_key = os.getenv("NewsApiKey")
        api_url = os.getenv("NewsApiUrl")

        if not api_key:
            _logger.error(
                "NewsAPI key not configured in environment variables")
            raise UserError(
                'NewsAPI key not configured in environment variables')

        if not api_url:
            _logger.error(
                "NewsAPI URL not configured in environment variables")
            raise UserError(
                'NewsAPI URL not configured in environment variables')

        try:
            to_date = datetime.now()
            # from_date = self.last_scan_date or (to_date - timedelta(years=3))
            from_date = self.last_scan_date or (
                to_date - relativedelta(days=30))

            # from_date = self.last_scan_date or to_date

            params = {
                'q': self._prepare_search_query(),
                'apiKey': api_key,
                'language': 'en',
                'from': from_date.date().isoformat(),
                'to': to_date.date().isoformat(),
                'sortBy': 'publishedAt'
            }

            _logger.info(
                f"Initiating NewsAPI request for partner: {self.partner_id.name} date is from {from_date} .. to {to_date} ...from date format {from_date.date().isoformat()}  to date format {to_date.date().isoformat()}")

            try:
                response = requests.get(api_url, params=params, timeout=10)
                response.raise_for_status()

                data = response.json()
                _logger.info(f"response from api {data}")
                _logger.info(
                    f"Successfully fetched {len(data.get('articles', []))} articles for partner: {self.partner_id.name}")
                return data

            except Timeout:
                _logger.error(
                    f"Request timeout for partner {self.partner_id.name}")
                raise UserError(
                    "NewsAPI request timed out. Please try again later.")

            except ConnectionError:
                _logger.error(
                    f"Connection error while fetching news for partner {self.partner_id.name}")
                raise UserError(
                    "Failed to connect to NewsAPI. Please check your internet connection.")

            except HTTPError as http_err:
                _logger.error(f"HTTP error occurred: {http_err}")

        except Exception as e:
            _logger.error(
                f"Unexpected error in _fetch_news_articles: {str(e)}", exc_info=True)
            raise UserError(f"Failed to fetch news articles: {str(e)}")

    def _calculate_risk_score(self, article, matched_keywords):
        """Calculate risk score based on matched keywords with error handling"""
        try:
            base_score = 0
            keyword_scores = {k.name: k.risk_score for k in self.keyword_id}

            for keyword in matched_keywords:
                score = keyword_scores.get(keyword, 0)
                base_score += score
                _logger.debug(
                    f"Keyword '{keyword}' contributed score: {score}")

            # final_score = min(100, base_score)
            final_score = (base_score)
            _logger.info(
                f"Calculated risk score {final_score} for article: {article.get('title', 'Unknown')}")
            return final_score

        except Exception as e:
            _logger.error(
                f"Error calculating risk score: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to calculate risk score: {str(e)}")

    def _create_alert(self, article, matched_keywords, risk_score):
        """Create adverse media alert with error handling"""
        try:
            if not all(key in article for key in ['title', 'url', 'description', 'content', 'publishedAt']):
                _logger.error(
                    f"Missing required fields in article data: {article}")
                raise ValidationError("Article data is incomplete")

            alert = self.env['adverse.media.alert'].create({
                'media_id': self.id,
                'name': article['title'],
                'source_url': article['url'],
                'description': article['description'],
                'content': article['content'],
                'source_date': fields.Date.from_string(article['publishedAt'][:10]),
                'risk_score': risk_score,
                'status': 'new',
                # 'matched_keyword': matched_keywords
            })

            _logger.info(
                f"Created alert ID {alert.id} for article: {article['title']}")
            return alert

        except Exception as e:
            _logger.error(f"Error creating alert: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to create alert: {str(e)}")

    def _notify_officers(self, new_alerts):
        """Send consolidated email notification to responsible officers"""
        try:
            template = self.env.ref(
                'compliance_management.alert_notification_template')
            if not template:
                _logger.error(
                    "Email template not found: compliance_management.alert_notification_template")
                raise ValidationError("Email template not found")

            officers = self.keyword_id.mapped('adverse_media_officers')
            if not officers:
                _logger.warning("No officers configured for alerts")
                return

            # Create action URL for filtered view
            base_url = self.env['ir.config_parameter'].sudo(
            ).get_param('web.base.url')
            action_id = self.env.ref(
                'compliance_management.action_adverse_media_alert').id
            action_url = f"{base_url}/web#action={action_id}&model=adverse.media.alert&view_type=list"

            # Get email from address
            email_from = os.getenv("EmailFrom")
            if not email_from:
                _logger.error("EmailFrom environment variable not configured")
                raise ValidationError("Email sender address not configured")

            # Debug logging
            _logger.info(f"Number of alerts to be sent: {len(new_alerts)}")
            for alert in new_alerts:
                _logger.info(
                    f"Alert in notification - Title: {alert.name}, Partner: {alert.partner_id.name}")

            # Prepare email values
            officers_name = ", ".join(officers.mapped('name')) or ""
            ctx = {
                'alerts': new_alerts,
                'action_url': action_url,
                'email_from': email_from,
                'officers_name': officers_name,
                'datetime': datetime.now().replace(microsecond=0)
            }

            officers_email = ", ".join(officers.mapped('email')) or ""

            try:
                # Render the template with context
                template_id = template.with_context(**ctx)

                # Get the rendered HTML content
                rendered_html = template_id._render_template(
                    template_id.body_html,
                    template_id.model,
                    [self.id],
                    engine='qweb',
                    add_context=ctx
                )[self.id]

                risk_scores = new_alerts.mapped('risk_score')
                highest_risk_score = max(risk_scores) if risk_scores else 0

                # Determine risk level based on the highest risk score
                alert_risk_level = None
                if highest_risk_score <= LOW_RISK_THRESHOLD:
                    alert_risk_level = "low"
                elif highest_risk_score <= MEDIUM_RISK_THRESHOLD:
                    alert_risk_level = "medium"
                else:
                    alert_risk_level = "high"

                # _logger.critical(
                #     f"Rendered html {rendered_html} /n .. Model name {self._description}.. /n Record id  {self.id}  ../n  new alert records to determine risk level {new_alerts}  ..highest risk score {highest_risk_score}  official risk level {alert_risk_level}")

                # Send email
                email_result = template_id.send_mail(
                    self.id,
                    force_send=True,
                    raise_exception=True,
                    email_values={
                        'email_to': officers_email,
                        'email_from': email_from,
                    }
                )

                mail = self.env['mail.mail'].browse(email_result)
                if mail.state == 'sent':
                    # insert into alert history table
                    self.env['alert.history'].sudo(flag=True).create({
                        "ref_id": f"{self._name},{self.id}",
                        'html_body': rendered_html,
                        'attachment_data':  None,
                        'attachment_link':  None,
                        'last_checked': fields.Datetime.now(),
                        'risk_rating': alert_risk_level if alert_risk_level else 'low',
                        'process_id': None,
                        'source': self._description,
                        'date_created': fields.Datetime.now(),
                        'email': officers_email
                        # Include other fields as needed
                    })
                    _logger.info(
                        f"Consolidated notification sent to officers ({officers_email})")

            except Exception as e:
                _logger.error(
                    f"Failed to send consolidated notification: {str(e)}")
                raise

        except Exception as e:
            _logger.error(
                f"Error in notification process: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to send notifications: {str(e)}")

    def scan_news_articles(self):
        """Main method to scan for news articles with comprehensive error handling"""
        for record in self:
            try:
                if record.monitoring_status != 'active':
                    _logger.info(
                        f"Skipping inactive record for partner: {record.partner_id.name}")
                    continue

                _logger.info(
                    f"Starting news scan for partner: {record.partner_id.name}")

                response_data = record._fetch_news_articles()
                if not response_data or 'articles' not in response_data:
                    _logger.error(
                        f"No articles found for partner: {record.partner_id.name}")
                    continue

                articles_processed = 0
                alerts_created = 0
                new_alerts = self.env['adverse.media.alert']

                for article in response_data['articles']:
                    # Create a savepoint for each article to isolate potential database issues
                    with self.env.cr.savepoint():
                        try:
                            articles_processed += 1
                            article_title = article.get('title', 'Unknown')

                            # Check for existing alert - wrapped in its own savepoint
                            try:
                                existing_alert = self.env['adverse.media.alert'].search([
                                    ('media_id', '=', record.id),
                                    ('name', '=', article_title)
                                ], limit=1)  # Adding limit=1 for efficiency
                            except Exception as db_error:
                                _logger.error(
                                    f"Database error checking for existing alert '{article_title}': {str(db_error)}")
                                continue  # Skip this article and move to the next one

                            if existing_alert:
                                _logger.info(
                                    f"Skipping duplicate article: {article_title}")
                                continue

                            # Process article
                            article_text = f"{article_title} {article.get('description', '')} {article.get('content', '')}"

                            matched_keywords = [
                                f"{k.name}"
                                for k in record.keyword_id
                                if record.partner_id.name.lower() in article_text.lower() and k.name.lower() in article_text.lower()
                            ]

                            _logger.info(
                                f"Matched keywords for '{article_title}': {matched_keywords}")

                            if matched_keywords:
                                risk_score = record._calculate_risk_score(
                                    article, matched_keywords)
                                alert = record._create_alert(
                                    article, matched_keywords, risk_score)

                                if alert:
                                    new_alerts |= alert
                                    alerts_created += 1
                                    _logger.info(
                                        f"Created alert for article: {article_title}")

                        except Exception as e:
                            _logger.error(
                                f"Error processing article '{article.get('title', 'Unknown')}': {str(e)}", exc_info=True)
                            # The savepoint will be rolled back automatically for this article
                            continue

                # Update scan date in a separate transaction
                try:
                    with self.env.cr.savepoint():
                        _logger.info(f"Scan completed for {record.partner_id.name}. "
                                     f"Processed {articles_processed} articles, created {alerts_created} alerts.")

                        if new_alerts:
                            record._notify_officers(new_alerts)
                            _logger.info(
                                f"Created {len(new_alerts)} new alerts for {record.partner_id.name}")

                        # Update the last scan date
                        record.write({
                            'last_scan_date': fields.Datetime.now(),
                            'next_scan_date': fields.Datetime.now() + timedelta(days=1)
                        })
                except Exception as update_error:
                    _logger.error(
                        f"Error updating scan dates for {record.partner_id.name}: {str(update_error)}", exc_info=True)

            except Exception as e:
                _logger.error(
                    f"Error scanning news for partner {record.partner_id.name}: {str(e)}", exc_info=True)
                continue

    @api.model
    def scan_adverse_media(self):
        """Cron job method to scan adverse media based on frequency"""
        current_time = fields.Datetime.now()

        # Find records that are active and due for scanning
        records = self.env['adverse.media'].search([
            ('monitoring_status', '=', 'active'),
            ('next_scan_date', '<=', current_time)
        ])

        for record in records:
            try:
                _logger.info(
                    f"Starting scan for partner: {record.partner_id.name}")
                record.scan_news_articles()

                # Update last scan date after successful scan
                record.write({
                    'last_scan_date': current_time
                })
                # next_scan_date will be automatically computed

            except Exception as e:
                _logger.error(
                    f"Error scanning news for {record.partner_id.name}: {str(e)}")

    @api.depends('last_scan_date', 'monitoring_frequency')
    def _compute_next_scan_date(self):
        """Compute the next scan date based on frequency and last scan"""
        for record in self:
            if not record.last_scan_date:
                record.next_scan_date = fields.Datetime.now()
                continue

            last_scan = fields.Datetime.from_string(record.last_scan_date)

            if record.monitoring_frequency == 'daily':
                next_scan = last_scan + timedelta(days=1)
            elif record.monitoring_frequency == 'weekly':
                next_scan = last_scan + timedelta(weeks=1)
            elif record.monitoring_frequency == 'monthly':
                # Add one month while handling month end cases
                next_month = last_scan.replace(day=1) + timedelta(days=32)
                next_scan = next_month.replace(
                    day=min(last_scan.day, (next_month.replace(day=1) - timedelta(days=1)).day))

            record.next_scan_date = fields.Datetime.to_string(next_scan)


class AdverseMediaAlert(models.Model):
    _name = 'adverse.media.alert'
    _description = 'Adverse Media Logs'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'create_date desc'

    media_id = fields.Many2one(
        'adverse.media', string='Screening Configuration', required=True, ondelete='cascade')
    partner_id = fields.Many2one(
        'res.partner', string='Partner', related='media_id.partner_id', store=True, index=True)
    partner_category = fields.Selection(
        related='partner_id.internal_category', store=True, index=True)
    name = fields.Char(string='Alert Title', required=True, tracking=True)

    source_url = fields.Char(string='Source URL', tracking=True)
    description = fields.Text(string='Description', tracking=True)
    content = fields.Text(string='Description', tracking=True, index=True)
    source_date = fields.Date(string='Publication Date')
    status = fields.Selection([
        ('new', 'New'),
        ('under_review', 'Under Review'),
        ('confirmed', 'Confirmed Risk'),
        ('closed', 'Closed')
    ], string='Status', default='new', tracking=True, inverse='_inverse_status')
    risk_score = fields.Integer(string='Risk Score', default=1, index=True)

    risk_score_decoration = fields.Char(
        compute='_compute_risk_score_decoration')
    _sql_constraints = [
        ('name_uniq', 'unique (media_id, name)',
         "Alert title must be unique for each media configuration!"),
    ]

    # matched_keyword = fields.Many2one(
    #     'media.keyword', string='Matched Keyword', ondelete='cascade')

    @api.depends('risk_score')
    def _compute_risk_score_decoration(self):
        for record in self:
            if record.risk_score <= 10:  # Match with 'low'
                record.risk_score_decoration = 'success'
            elif 10 < record.risk_score <= 19:  # Match with 'medium'
                record.risk_score_decoration = 'warning'
            else:  # Match with 'high'
                record.risk_score_decoration = 'danger'

    def _inverse_status(self):
        """Triggered when status field is changed"""
        for record in self:
            _logger.info(
                f"_inverse_status triggered for record {record.id}, status: {record.status}")
            if record.status == 'confirmed':
                self.update_partner_risk()

    def update_partner_risk(self):
        """ method to update partner risk"""
        _logger.info(f"update_partner_risk called for records: {self}")
        for record in self:
            _logger.info(
                f"Processing record {record.id}, status: {record.status}, media_id: {record.media_id}, partner_id: {record.media_id.partner_id if record.media_id else None}")

            if record.status == 'confirmed' and record.media_id and record.media_id.partner_id:
                _logger.info(
                    f"Conditions met, searching for media_keyword with risk_score: {record.risk_score}")
                media_keyword = self.env['media.keyword'].search(
                    [('risk_score', '=', record.risk_score)], limit=1)

                _logger.info(f"Found media_keyword: {media_keyword}")
                if media_keyword:
                    _logger.info(
                        f"Updating partner {record.media_id.partner_id} with risk_score: {media_keyword.risk_score}, risk_level: {media_keyword.media_risk_level}")

                    # Use direct SQL update to avoid triggering write() method
                    self.env.cr.execute(
                        """UPDATE res_partner SET risk_score = %s, risk_level = %s 
                        WHERE id = %s""",
                        (media_keyword.risk_score, media_keyword.media_risk_level,
                         record.media_id.partner_id.id)
                    )

                    # Invalidate cache for the partner
                    record.media_id.partner_id.invalidate_recordset(
                        ['risk_score', 'risk_level'])

                    _logger.info(f"Partner updated successfully")


class MediaKeyword(models.Model):
    _name = 'media.keyword'  # Model name, customize as needed
    _description = 'Keyword'

    name = fields.Char(string='Name', required=True)
    code = fields.Char(string='Code', required=True)
    risk_score = fields.Float(string='Risk Score', default=0.0)
    media_risk_level = fields.Selection([
        ('low', 'Low'),
        ('medium', 'Medium'),
        ('high', 'High'),
    ], string='Risk Level', default='low', tracking=True)
    adverse_media_officers = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        'adverse_media_officers',
        string="Officer(s) Responsible",
        tracking=True,
    )

    risk_score_decoration = fields.Char(
        compute='_compute_risk_score_decoration')
    active = fields.Boolean(default=True, tracking=True)

    @api.depends('risk_score')
    def _compute_risk_score_decoration(self):
        for record in self:
            if record.risk_score <= 10:  # Match with 'low'
                record.risk_score_decoration = 'success'
            elif 10 < record.risk_score <= 19:  # Match with 'medium'
                record.risk_score_decoration = 'warning'
            else:  # Match with 'high'
                record.risk_score_decoration = 'danger'

    @api.onchange('risk_score')
    def _onchange_risk_score(self):
        if self.risk_score <= LOW_RISK_THRESHOLD:
            self.media_risk_level = "low"
        elif self.risk_score <= MEDIUM_RISK_THRESHOLD:
            self.media_risk_level = "medium"
        else:
            self.media_risk_level = "high"
