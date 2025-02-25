from odoo import models, fields, api
from odoo.exceptions import AccessError
from dateutil.relativedelta import relativedelta
from odoo.exceptions import UserError, ValidationError
import logging
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


load_dotenv()
_logger = logging.getLogger(__name__)


class AdverseMedia(models.Model):
    _name = 'adverse.media'
    _description = 'Adverse Media Screening'
    _order = "id desc"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    partner_id = fields.Many2one(
        'res.partner', string='Partner', required=True, index=True)
    partner_risk_score = fields.Float(
        related='partner_id.risk_score', string="Risk Score")
    partner_risk_level = fields.Char(
        related='partner_id.risk_level', string="Risk Level")

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

    last_scan_date = fields.Datetime(string='Last Scan')
    next_scan_date = fields.Datetime(string='Next Scan Date', compute='_compute_next_scan_date', store=True)
    
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
                "You do not have the necessary permissions to view the Reply Log.")

    def _prepare_search_query(self):
        """Prepare search query for NewsAPI"""
        try:
            partner_name = self.partner_id.name
            if not partner_name:
                _logger.error(f"Missing partner name for record ID: {self.id}")
                raise ValidationError(
                    "Partner name is required for media screening")

            keywords = self.keyword_id.mapped('name')
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
                'status': 'new'
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

                # Send email
                template_id.send_mail(
                    self.id,
                    force_send=True,
                    email_values={
                        'email_to': officers_email,
                        'email_from': email_from,
                    }
                )
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
                    try:
                        articles_processed += 1

                        # Check for existing alert
                        existing_alert = self.env['adverse.media.alert'].search([
                            ('media_id', '=', record.id),
                            ('name', '=', article['title'])
                        ])

                        if existing_alert:
                            _logger.info(
                                f"Skipping duplicate article: {article['title']}")
                            continue

                        # Process article
                        article_text = f"{article['title']} {article['description']} {article['content']}"

                        matched_keywords = [
                            f"{k.name}"
                            for k in record.keyword_id
                            if record.partner_id.name.lower() in article_text.lower() and k.name.lower() in article_text.lower()
                        ]

                        _logger.info(f"matched keywoods {matched_keywords}")

                        if matched_keywords:
                            risk_score = record._calculate_risk_score(
                                article, matched_keywords)
                            alert = record._create_alert(
                                article, matched_keywords, risk_score)

                            if alert:
                                new_alerts |= alert
                                _logger.info(
                                    f"Created alert for article: {article['title']}")

                    except Exception as e:
                        _logger.error(
                            f"Error processing article {article.get('title', 'Unknown')}: {str(e)}", exc_info=True)
                        continue

                _logger.info(f"Scan completed for {record.partner_id.name}. "
                             f"Processed {articles_processed} articles, created {new_alerts} alerts.")
                if new_alerts:
                    record._notify_officers(new_alerts)
                    _logger.info(
                        f"Created {len(new_alerts)} new alerts for {record.partner_id.name}")

                record.last_scan_date = fields.Datetime.now()

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
                _logger.info(f"Starting scan for partner: {record.partner_id.name}")
                record.scan_news_articles()
                
                # Update last scan date after successful scan
                record.write({
                    'last_scan_date': current_time
                })
                # next_scan_date will be automatically computed
                
            except Exception as e:
                _logger.error(f"Error scanning news for {record.partner_id.name}: {str(e)}")
                           
                              
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
                next_scan = next_month.replace(day=min(last_scan.day, (next_month.replace(day=1) - timedelta(days=1)).day))
            
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
    ], string='Status', default='new', tracking=True)
    risk_score = fields.Integer(string='Risk Score', default=1, index=True)
    
    risk_score_decoration = fields.Char(
        compute='_compute_risk_score_decoration')

    @api.depends('risk_score')
    def _compute_risk_score_decoration(self):
        for record in self:
            if record.risk_score <= 10:  # Match with 'low'
                record.risk_score_decoration = 'success'
            elif 10 < record.risk_score <= 19:  # Match with 'medium'
                record.risk_score_decoration = 'warning'
            else:  # Match with 'high'
                record.risk_score_decoration = 'danger'
    

    _sql_constraints = [
        ('name_uniq', 'unique (media_id, name)',
         "Alert title must be unique for each media configuration!"),
    ]


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
    
    

    @api.depends('risk_score')
    def _compute_risk_score_decoration(self):
        for record in self:
            if record.risk_score <= 10:  # Match with 'low'
                record.risk_score_decoration = 'success'
            elif 10 < record.risk_score <= 19:  # Match with 'medium'
                record.risk_score_decoration = 'warning'
            else:  # Match with 'high'
                record.risk_score_decoration = 'danger'