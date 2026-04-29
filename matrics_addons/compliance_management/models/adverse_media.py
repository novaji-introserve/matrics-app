from odoo import models, fields, api
from odoo.exceptions import AccessError
from dateutil.relativedelta import relativedelta
from odoo.exceptions import UserError, ValidationError
from datetime import timedelta, datetime, time
import re
import threading
from dotenv import load_dotenv
import os
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import hashlib
import requests
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
from odoo.modules.module import get_module_resource
import base64
import logging


# Load .env from icomply_odoo root (two levels up from this file's models/ directory)
load_dotenv(Path(__file__).resolve().parent.parent.parent / '.env')
_logger = logging.getLogger(__name__)

try:
    import feedparser
    _FEEDPARSER_AVAILABLE = True
except ImportError:
    _FEEDPARSER_AVAILABLE = False
    _logger_tmp = logging.getLogger(__name__)
    _logger_tmp.warning("feedparser not installed — RSS feed scanning unavailable")

_ARTICLE_TYPE_MULTIPLIERS = {
    'conviction': 1.5,
    'enforcement': 1.3,
    'investigation': 1.0,
    'accusation': 0.8,
    'opinion': 0.5,
    'other': 1.0,
}


class AdverseMedia(models.Model):
    _name = 'adverse.media'
    _description = 'Adverse Media Screening'
    _order = "id desc"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    def name_get(self):
        return [(rec.id, rec.partner_id.name or f'adverse.media,{rec.id}') for rec in self]

    partner_id = fields.Many2one(
        'res.partner', string='Partner', tracking=True, required=True, index=True, ondelete='cascade',
        domain="[('origin', 'in', ['demo', 'test', 'prod'])]")
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
    ], string='Monitoring Frequency', default='daily', tracking=True)

    last_scan_date = fields.Datetime(string='Last Scan', tracking=True)
    next_scan_date = fields.Datetime(
        string='Next Scan Date', tracking=True, compute='_compute_next_scan_date', store=True)

    keyword_id = fields.Many2many(
        'media.keyword', string='Keywords for Monitoring', required=True,
        tracking=True, index=True, default=lambda self: self._default_keywords())

    alert_ids = fields.One2many(
        'adverse.media.alert', 'media_id', string='Alerts', index=True)

    scan_log_ids = fields.One2many(
        'adverse.media.scan.log', 'media_id', string='Scan History')

    scan_status = fields.Selection([
        ('idle', 'Idle'),
        ('queued', 'Queued'),
        ('running', 'Running'),
        ('error', 'Error'),
    ], string='Scan Status', default='idle', tracking=True)

    last_scan_error = fields.Text(string='Last Scan Error')

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')

    risk_score_decoration = fields.Char(compute='_compute_risk_score_decoration')

    def _default_keywords(self):
        return self.env['media.keyword'].search([]).ids

    @api.depends('partner_risk_score')
    def _compute_risk_score_decoration(self):
        for record in self:
            if record.partner_risk_score <= float(self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
                record.risk_score_decoration = 'success'
            elif record.partner_risk_score <= float(self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
                record.risk_score_decoration = 'warning'
            else:
                record.risk_score_decoration = 'danger'

    def open_media_log(self):
        try:
            action = self.env.ref(
                "compliance_management.action_adverse_media_alert").sudo().read()[0]
            action["domain"] = [("media_id", "=", self.id)]
            return action
        except AccessError:
            raise AccessError(
                "You do not have the necessary permissions to view the Media Log.")

    @api.model
    def enrol_partners(self, partners):
        """Bulk-enrol partners in adverse media monitoring.

        For company partners, also enrols their direct contacts (type='contact').
        Returns a display_notification action with a summary.
        """
        # Gather all candidates: selected partners + their company contacts
        all_candidates = self.env['res.partner']
        for partner in partners:
            all_candidates |= partner
            if partner.is_company:
                all_candidates |= partner.child_ids.filtered(lambda c: c.type == 'contact')

        # Single query to find already-enrolled partner IDs
        already_enrolled_ids = set(
            self.search([('partner_id', 'in', all_candidates.ids)]).mapped('partner_id.id')
        )

        to_enrol = all_candidates.filtered(lambda p: p.id not in already_enrolled_ids)
        if to_enrol:
            self.create([{'partner_id': p.id} for p in to_enrol])

        enrolled = len(to_enrol)
        skipped = len(all_candidates) - enrolled
        msg = f'{enrolled} partner(s) enrolled in adverse media monitoring.'
        if skipped:
            msg += f' {skipped} already monitored.'

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Enrolment Complete',
                'message': msg,
                'sticky': False,
                'type': 'success',
            },
        }

    def _prepare_search_query(self):
        """Build a NewsAPI boolean query: (name OR alias1 OR alias2) AND (kw1 OR kw2 OR ...)"""
        try:
            partner_name = self.partner_id.name
            if not partner_name:
                raise ValidationError("Partner name is required for media screening")

            keywords = self.keyword_id.mapped('name')
            if not keywords:
                raise ValidationError("At least one keyword is required for media screening")

            alias_names = [
                a.strip() for a in (self.partner_id.alias_names or '').split(',') if a.strip()
            ]
            name_parts = [f'"{partner_name}"'] + [f'"{a}"' for a in alias_names]
            name_clause = f'({" OR ".join(name_parts)})' if len(name_parts) > 1 else name_parts[0]

            keyword_clause = ' OR '.join(f'"{k}"' for k in keywords)
            query = f'{name_clause} AND ({keyword_clause})'

            _logger.info(f"Generated search query for partner {partner_name}: {query}")
            return query

        except Exception as e:
            _logger.error(f"Error preparing search query: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to prepare search query: {str(e)}")

    def _fetch_news_articles(self):
        """Fetch news articles from NewsAPI with error handling."""
        api_key = os.getenv("NewsApiKey")
        api_url = os.getenv("NewsApiUrl")

        if not api_key:
            raise UserError('NewsAPI key not configured in environment variables')
        if not api_url:
            raise UserError('NewsAPI URL not configured in environment variables')

        try:
            to_date = datetime.now()
            from_date = self.last_scan_date or (to_date - relativedelta(days=30))

            params = {
                'q': self._prepare_search_query(),
                'apiKey': api_key,
                'language': 'en',
                'from': from_date.date().isoformat(),
                'to': to_date.date().isoformat(),
                'sortBy': 'publishedAt'
            }

            _logger.info(
                f"Initiating NewsAPI request for partner: {self.partner_id.name} "
                f"date range: {from_date.date().isoformat()} to {to_date.date().isoformat()}")

            try:
                response = requests.get(api_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                _logger.info(f"Fetched {len(data.get('articles', []))} articles for: {self.partner_id.name}")
                return data

            except Timeout:
                _logger.error(f"Request timeout for partner {self.partner_id.name}")
                raise UserError("NewsAPI request timed out. Please try again later.")

            except ConnectionError:
                _logger.error(f"Connection error for partner {self.partner_id.name}")
                raise UserError("Failed to connect to NewsAPI. Please check your internet connection.")

            except HTTPError as http_err:
                _logger.error(f"HTTP error occurred: {http_err}")

        except Exception as e:
            _logger.error(f"Unexpected error in _fetch_news_articles: {str(e)}", exc_info=True)
            raise UserError(f"Failed to fetch news articles: {str(e)}")

    def _fetch_rss_articles(self, source):
        """Fetch and normalise articles from an RSS/Atom feed."""
        if not _FEEDPARSER_AVAILABLE:
            raise UserError("feedparser is not installed. Run: pip install feedparser")
        try:
            from time import mktime
            feed = feedparser.parse(source.url)
            if feed.bozo and not feed.entries:
                _logger.warning(f"RSS parse error for {source.name} ({source.url}): {feed.bozo_exception}")
                return []

            feed_title = feed.feed.get('title', source.name)
            articles = []
            for entry in feed.entries:
                pub_date = datetime.now().isoformat()
                if getattr(entry, 'published_parsed', None):
                    try:
                        pub_date = datetime.utcfromtimestamp(mktime(entry.published_parsed)).isoformat()
                    except Exception:
                        pass

                content = ''
                if getattr(entry, 'content', None):
                    content = entry.content[0].get('value', '')

                articles.append({
                    'title': entry.get('title', ''),
                    'url': entry.get('link', ''),
                    'publishedAt': pub_date,
                    'description': entry.get('summary', ''),
                    'content': content,
                    'source': {'name': feed_title},
                })

            _logger.info(f"Fetched {len(articles)} articles from RSS: {source.name}")
            source.sudo().write({'last_fetched': fields.Datetime.now()})
            return articles

        except Exception as e:
            _logger.error(f"Error fetching RSS feed {source.url}: {e}", exc_info=True)
            return []

    def _gather_articles(self):
        """Return articles from all active RSS sources, or fall back to NewsAPI."""
        sources = self.env['media.source'].search([('active', '=', True)])
        if not sources:
            _logger.info("No active RSS sources — falling back to NewsAPI")
            data = self._fetch_news_articles()
            return data.get('articles', []) if data else []

        all_articles = []
        for source in sources:
            all_articles.extend(self._fetch_rss_articles(source))
        _logger.info(f"Gathered {len(all_articles)} total articles from {len(sources)} RSS source(s)")
        return all_articles

    def _get_matched_keywords(self, article_text):
        """Return keyword records matching the article, with exclusion-term filtering."""
        partner_name = self.partner_id.name
        alias_names = [
            a.strip() for a in (self.partner_id.alias_names or '').split(',') if a.strip()
        ]
        all_names = [partner_name] + alias_names

        if not any(n.lower() in article_text.lower() for n in all_names):
            return self.env['media.keyword']

        matched = self.env['media.keyword']
        for kw in self.keyword_id:
            if kw.name.lower() not in article_text.lower():
                continue
            exclusions = [e.strip().lower() for e in (kw.exclusion_terms or '').split(',') if e.strip()]
            if exclusions and any(excl in article_text.lower() for excl in exclusions):
                _logger.debug(f"Keyword '{kw.name}' skipped due to exclusion term match")
                continue
            matched |= kw
        return matched

    def _calculate_risk_score(self, article, matched_keywords):
        """Sum keyword scores × recency factor. Returns (base_score, final_score)."""
        try:
            base_score = sum(k.risk_score for k in matched_keywords)

            recency_factor = 1.0
            pub_date_str = article.get('publishedAt', '')
            if pub_date_str:
                try:
                    pub_date = datetime.fromisoformat(pub_date_str[:10])
                    age_days = (datetime.now() - pub_date).days
                    if age_days < 30:
                        recency_factor = 1.0
                    elif age_days < 90:
                        recency_factor = 0.8
                    elif age_days < 365:
                        recency_factor = 0.6
                    else:
                        recency_factor = 0.4
                except (ValueError, TypeError):
                    pass

            score = base_score * recency_factor
            max_threshold = float(
                self.env['res.compliance.settings'].get_setting('maximum_risk_threshold') or 9
            )
            final_score = min(score, max_threshold)
            _logger.info(
                f"Risk score for '{article.get('title', 'Unknown')}': "
                f"base={base_score:.2f}, recency={recency_factor}, final={final_score:.2f}"
            )
            return base_score, final_score
        except Exception as e:
            _logger.error(f"Error calculating risk score: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to calculate risk score: {str(e)}")

    def _compute_priority(self, risk_score):
        """Derive alert priority (0–4) from risk score."""
        high = float(self.env['res.compliance.settings'].get_setting('high_risk_threshold') or 9)
        escalation = float(self.env['res.compliance.settings'].get_setting('am_escalation_threshold') or 7)
        medium = float(self.env['res.compliance.settings'].get_setting('medium_risk_threshold') or 6.9)
        low = float(self.env['res.compliance.settings'].get_setting('low_risk_threshold') or 3.9)
        if risk_score >= high:
            return '0'
        elif risk_score >= escalation:
            return '1'
        elif risk_score >= medium:
            return '2'
        elif risk_score >= low:
            return '3'
        return '4'

    def _compute_review_deadline(self, priority):
        """Return deadline datetime based on priority and SLA settings."""
        sla_map = {
            '0': int(self.env['res.compliance.settings'].get_setting('am_sla_critical_hours') or 4),
            '1': int(self.env['res.compliance.settings'].get_setting('am_sla_high_hours') or 24),
            '2': int(self.env['res.compliance.settings'].get_setting('am_sla_medium_hours') or 48),
            '3': int(self.env['res.compliance.settings'].get_setting('am_sla_low_hours') or 72),
            '4': int(self.env['res.compliance.settings'].get_setting('am_sla_low_hours') or 72),
        }
        return fields.Datetime.now() + timedelta(hours=sla_map.get(priority, 72))

    def _create_alert(self, article, matched_keywords, base_risk_score, risk_score, content_hash, scan_log=None):
        """Create an adverse media alert record."""
        try:
            if not article.get('title') or not article.get('url') or not article.get('publishedAt'):
                _logger.error(f"Missing required fields in article data: {article}")
                raise ValidationError("Article data is incomplete")

            source_info = article.get('source', {})
            source_name = source_info.get('name', '') if isinstance(source_info, dict) else ''

            priority = self._compute_priority(risk_score)
            review_deadline = self._compute_review_deadline(priority)

            vals = {
                'media_id': self.id,
                'name': article['title'],
                'source_url': article['url'],
                'source_name': source_name,
                'description': article.get('description') or '',
                'content': article.get('content') or '',
                'source_date': fields.Date.from_string(article['publishedAt'][:10]),
                'base_risk_score': base_risk_score,
                'risk_score': risk_score,
                'priority': priority,
                'review_deadline': review_deadline,
                'status': 'new',
                'content_hash': content_hash,
                'matched_keyword_ids': [(6, 0, matched_keywords.ids)],
            }
            if scan_log:
                vals['scan_log_id'] = scan_log.id

            alert = self.env['adverse.media.alert'].create(vals)
            _logger.info(f"Created alert ID {alert.id} for article: {article['title']}")
            return alert

        except Exception as e:
            _logger.error(f"Error creating alert: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to create alert: {str(e)}")

    def _notify_officers(self, new_alerts):
        """Send consolidated email notification to responsible officers."""
        try:
            template = self.env.ref('compliance_management.alert_notification_template')
            if not template:
                raise ValidationError("Email template not found")

            officers = self.keyword_id.mapped('adverse_media_officers')
            if not officers:
                _logger.warning("No officers configured for alerts")
                return

            base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
            action_id = self.env.ref('compliance_management.action_adverse_media_alert').id
            action_url = f"{base_url}/web#action={action_id}&model=adverse.media.alert&view_type=list"

            email_from = os.getenv("EmailFrom")
            if not email_from:
                raise ValidationError("Email sender address not configured")

            _logger.info(f"Sending notification for {len(new_alerts)} alert(s)")

            officers_name = ", ".join(officers.mapped('name')) or ""
            ctx = {
                'alerts': new_alerts,
                'action_url': action_url,
                'email_from': email_from,
                'officers_name': officers_name,
                'datetime': datetime.now().replace(microsecond=0),
                'company_logo': self.get_company_logo(base_url)
            }

            officers_email = ", ".join(officers.mapped('email')) or ""

            try:
                template_id = template.with_context(**ctx)

                rendered_html = template_id._render_template(
                    template_id.body_html,
                    template_id.model,
                    [self.id],
                    engine='qweb',
                    add_context=ctx
                )[self.id]

                risk_scores = new_alerts.mapped('risk_score')
                highest_risk_score = max(risk_scores) if risk_scores else 0

                if highest_risk_score <= float(self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
                    alert_risk_level = "low"
                elif highest_risk_score <= float(self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
                    alert_risk_level = "medium"
                else:
                    alert_risk_level = "high"

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
                    self.env['alert.history'].sudo().create({
                        "ref_id": self._name,
                        'html_body': rendered_html,
                        'attachment_data': None,
                        'attachment_link': None,
                        'last_checked': fields.Datetime.now(),
                        'risk_rating': alert_risk_level,
                        'process_id': None,
                        'source': self._description,
                        'date_created': fields.Datetime.now(),
                        'email': officers_email
                    })
                    _logger.info(f"Notification sent to: {officers_email}")

            except Exception as e:
                _logger.error(f"Failed to send notification: {str(e)}")
                raise

        except Exception as e:
            _logger.error(f"Error in notification process: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to send notifications: {str(e)}")

    def scan_news_articles(self):
        """Trigger adverse media scan. Enqueues a background job if queue_job is available,
        otherwise runs synchronously."""
        queue_job_installed = bool(
            self.env['ir.module.module'].sudo().search(
                [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1
            )
        )

        queued_count = 0
        for record in self:
            if record.monitoring_status != 'active':
                _logger.info(f"Skipping inactive record for partner: {record.partner_id.name}")
                continue
            record.write({'scan_status': 'queued', 'last_scan_error': False})
            if queue_job_installed:
                record.with_delay(
                    priority=5,
                    description=f"Adverse media scan: {record.partner_id.name}"
                ).run_scan_job()
                queued_count += 1
            else:
                record.run_scan_job()

        if queue_job_installed and queued_count:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Scan Queued',
                    'message': f'Adverse media scan queued for {queued_count} partner(s). Results will appear in the media log shortly.',
                    'type': 'info',
                    'sticky': False,
                }
            }

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Screening Complete',
                'message': 'Adverse media screening completed. Check the media log for results.',
                'type': 'success',
                'sticky': False,
            }
        }

    def run_scan_job(self, triggered_by='manual'):
        """Execute the adverse media scan for a single record. Called by queue_job or directly."""
        self.ensure_one()

        scan_log = self.env['adverse.media.scan.log'].create({
            'media_id': self.id,
            'triggered_by': triggered_by,
        })

        self.write({'scan_status': 'running'})

        articles_fetched = 0
        articles_skipped = 0
        alerts_created = 0
        new_alerts = self.env['adverse.media.alert']
        article_log = []

        try:
            _logger.info(f"Starting scan for partner: {self.partner_id.name}")

            articles = self._gather_articles()
            if not articles:
                _logger.warning(f"No articles returned for partner: {self.partner_id.name}")
                scan_log.write({'status': 'success'})
                self.write({'scan_status': 'idle', 'last_scan_date': fields.Datetime.now()})
                return

            articles_fetched = len(articles)

            for article in articles:
                with self.env.cr.savepoint():
                    try:
                        article_title = article.get('title') or 'Unknown'
                        article_text = (
                            f"{article_title} "
                            f"{article.get('description', '') or ''} "
                            f"{article.get('content', '') or ''}"
                        )
                        article_source = (
                            article.get('source_name') or
                            article.get('source', {}).get('name', '') or ''
                        )
                        article_url = article.get('url', '')
                        raw_date = article.get('publishedAt') or article.get('published')
                        try:
                            from dateutil import parser as dateparser
                            article_published = dateparser.parse(raw_date) if raw_date else False
                        except Exception:
                            article_published = False

                        def _log(outcome, risk_score=0.0, keywords=''):
                            article_log.append({
                                'scan_log_id': scan_log.id,
                                'title': article_title[:255],
                                'source_name': article_source[:128],
                                'url': article_url[:512],
                                'published_at': article_published,
                                'outcome': outcome,
                                'risk_score': risk_score,
                                'matched_keywords': keywords[:255],
                            })

                        content_hash = hashlib.sha256(
                            f"{article_url}{article_title}".encode()
                        ).hexdigest()

                        existing = self.env['adverse.media.alert'].search([
                            ('media_id', '=', self.id),
                            ('content_hash', '=', content_hash),
                        ], limit=1)
                        if existing:
                            articles_skipped += 1
                            _log('duplicate')
                            _logger.debug(f"Skipping duplicate article: {article_title}")
                            continue

                        matched_keywords = self._get_matched_keywords(article_text)
                        _logger.info(f"Matched keywords for '{article_title}': {matched_keywords.mapped('name')}")

                        if not matched_keywords:
                            _log('no_match')
                            continue

                        base_score, risk_score = self._calculate_risk_score(article, matched_keywords)
                        kw_names = ', '.join(matched_keywords.mapped('name'))

                        min_threshold = float(
                            self.env['res.compliance.settings'].get_setting('minimum_alert_threshold') or 0
                        )
                        if risk_score < min_threshold:
                            articles_skipped += 1
                            _log('below_threshold', risk_score, kw_names)
                            _logger.debug(
                                f"Skipping article below minimum threshold "
                                f"(score={risk_score} < threshold={min_threshold}): {article_title}"
                            )
                            continue

                        url_domain = article_url.split('/')[2] if '//' in article_url else ''
                        suppression = self.env['adverse.media.suppression'].sudo().search([
                            ('partner_id', '=', self.partner_id.id),
                            ('active', '=', True),
                            '|',
                            ('url_domain', '=', url_domain),
                            ('keyword_id', 'in', matched_keywords.ids),
                        ], limit=1)
                        if suppression:
                            articles_skipped += 1
                            _log('suppressed', risk_score, kw_names)
                            _logger.debug(
                                f"Article suppressed by rule {suppression.id}: {article_title}")
                            continue

                        alert = self._create_alert(
                            article, matched_keywords, base_score, risk_score,
                            content_hash, scan_log=scan_log,
                        )
                        if alert:
                            new_alerts |= alert
                            alerts_created += 1
                            _log('alert', risk_score, kw_names)
                        else:
                            _log('duplicate', risk_score, kw_names)

                    except Exception as e:
                        article_log.append({
                            'scan_log_id': scan_log.id,
                            'title': (article.get('title') or 'Unknown')[:255],
                            'outcome': 'error',
                            'matched_keywords': str(e)[:255],
                        })
                        _logger.error(
                            f"Error processing article '{article.get('title', 'Unknown')}': {str(e)}",
                            exc_info=True)

            _logger.info(
                f"Scan complete for {self.partner_id.name}: "
                f"{articles_fetched} fetched, {articles_skipped} skipped, {alerts_created} alerts created")

            if article_log:
                self.env['adverse.media.scan.log.article'].sudo().create(article_log)

            if new_alerts:
                try:
                    self.with_context(news_scan=True)._notify_officers(new_alerts)
                except Exception as notify_error:
                    _logger.error(f"Notification failed: {str(notify_error)}", exc_info=True)

            self.write({'scan_status': 'idle', 'last_scan_date': fields.Datetime.now()})
            scan_log.write({
                'status': 'success',
                'articles_fetched': articles_fetched,
                'articles_skipped': articles_skipped,
                'alerts_created': alerts_created,
            })

        except Exception as e:
            error_msg = str(e)
            _logger.error(f"Scan failed for {self.partner_id.name}: {error_msg}", exc_info=True)
            self.write({'scan_status': 'error', 'last_scan_error': error_msg})
            scan_log.write({
                'status': 'failed',
                'error_message': error_msg,
                'articles_fetched': articles_fetched,
            })

    @api.model
    def scan_adverse_media(self):
        """Cron job: find records due for scanning and enqueue or run each one."""
        current_time = fields.Datetime.now()

        records = self.env['adverse.media'].search([
            ('monitoring_status', '=', 'active'),
            ('next_scan_date', '<=', current_time),
        ])

        queue_job_installed = bool(
            self.env['ir.module.module'].sudo().search(
                [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1
            )
        )

        for record in records:
            try:
                _logger.info(f"Queueing scan for partner: {record.partner_id.name}")
                record.write({'scan_status': 'queued'})
                if queue_job_installed:
                    record.with_delay(priority=5).run_scan_job(triggered_by='cron')
                else:
                    record.run_scan_job(triggered_by='cron')
            except Exception as e:
                _logger.error(f"Error queueing scan for {record.partner_id.name}: {str(e)}")

    @api.depends('last_scan_date', 'monitoring_frequency')
    def _compute_next_scan_date(self):
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
                next_month = last_scan.replace(day=1) + timedelta(days=32)
                next_scan = next_month.replace(
                    day=min(last_scan.day, (next_month.replace(day=1) - timedelta(days=1)).day))
            else:
                next_scan = last_scan + timedelta(days=1)

            record.next_scan_date = fields.Datetime.to_string(next_scan)

    def action_open_scan_logs(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Scan History',
            'res_model': 'adverse.media.scan.log',
            'view_mode': 'tree,form',
            'domain': [('media_id', '=', self.id)],
        }

    def get_company_logo(self, base_url):
        company = self.env.user.company_id
        return f"{base_url}/web/image/res.company/{company.id}/logo_web"


class AdverseMediaScanLog(models.Model):
    _name = 'adverse.media.scan.log'
    _description = 'Adverse Media Scan Log'
    _order = 'scan_date desc'

    media_id = fields.Many2one(
        'adverse.media', string='Screening Record', ondelete='cascade', required=True, index=True)
    scan_date = fields.Datetime(string='Scan Date', default=fields.Datetime.now, readonly=True)
    triggered_by = fields.Selection([
        ('cron', 'Scheduled'),
        ('manual', 'Manual'),
        ('event', 'Event'),
    ], string='Triggered By', default='manual', readonly=True)
    articles_fetched = fields.Integer(string='Articles Fetched', default=0, readonly=True)
    articles_skipped = fields.Integer(string='Skipped (Duplicates)', default=0, readonly=True)
    alerts_created = fields.Integer(string='Alerts Created', default=0, readonly=True)
    status = fields.Selection([
        ('success', 'Success'),
        ('partial', 'Partial'),
        ('failed', 'Failed'),
    ], string='Status', default='success', readonly=True)
    error_message = fields.Text(string='Error Details', readonly=True)
    article_ids = fields.One2many(
        'adverse.media.scan.log.article', 'scan_log_id', string='Article Log')
    article_count = fields.Integer(
        string='Article Count', compute='_compute_article_count')
    partner_id = fields.Many2one(
        'res.partner', related='media_id.partner_id', string='Partner', store=True)

    def _compute_article_count(self):
        for rec in self:
            rec.article_count = len(rec.article_ids)

    def name_get(self):
        result = []
        for rec in self:
            partner = rec.media_id.partner_id.name or ''
            date = rec.scan_date.strftime('%Y-%m-%d %H:%M') if rec.scan_date else ''
            result.append((rec.id, f'{partner} — {date}' if partner else date))
        return result

    def action_open_articles(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': f'Articles — {self.media_id.partner_id.name or "Scan"} ({self.scan_date.strftime("%Y-%m-%d") if self.scan_date else ""})',
            'res_model': 'adverse.media.scan.log.article',
            'view_mode': 'tree,form',
            'domain': [('scan_log_id', '=', self.id)],
            'context': {'search_default_group_outcome': 1},
        }


class AdverseMediaScanLogArticle(models.Model):
    _name = 'adverse.media.scan.log.article'
    _description = 'Scan Log — Article Entry'
    _order = 'id desc'

    scan_log_id = fields.Many2one(
        'adverse.media.scan.log', string='Scan Log', ondelete='cascade', required=True, index=True)
    title = fields.Char(string='Title', readonly=True)
    source_name = fields.Char(string='Source', readonly=True)
    url = fields.Char(string='URL', readonly=True)
    published_at = fields.Datetime(string='Published', readonly=True)
    outcome = fields.Selection([
        ('alert', 'Alert Created'),
        ('duplicate', 'Duplicate'),
        ('below_threshold', 'Below Threshold'),
        ('suppressed', 'Suppressed'),
        ('no_match', 'No Keyword Match'),
        ('error', 'Error'),
    ], string='Outcome', readonly=True)
    risk_score = fields.Float(string='Risk Score', readonly=True)
    matched_keywords = fields.Char(string='Matched Keywords', readonly=True)


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
    source_name = fields.Char(string='Source', tracking=True)
    description = fields.Text(string='Description', tracking=True)
    content = fields.Text(string='Media Content', tracking=True, index=True)
    content_hash = fields.Char(string='Content Hash', index=True)
    base_risk_score = fields.Float(string='Base Risk Score', readonly=True)
    scan_log_id = fields.Many2one(
        'adverse.media.scan.log', string='Scan Log', readonly=True, ondelete='set null')

    priority = fields.Selection([
        ('0', 'Critical'),
        ('1', 'High'),
        ('2', 'Medium'),
        ('3', 'Low'),
        ('4', 'Informational'),
    ], string='Priority', default='2', tracking=True)

    article_type = fields.Selection([
        ('accusation', 'Accusation/Allegation'),
        ('investigation', 'Under Investigation'),
        ('conviction', 'Convicted/Sentenced'),
        ('enforcement', 'Regulatory Enforcement'),
        ('opinion', 'Opinion/Commentary'),
        ('other', 'Other'),
    ], string='Article Type', tracking=True)

    resolution_status = fields.Selection([
        ('ongoing', 'Ongoing'),
        ('pending', 'Pending'),
        ('dismissed', 'Dismissed'),
        ('resolved', 'Resolved'),
    ], string='Resolution Status', tracking=True)

    disposition = fields.Selection([
        ('true_match', 'True Match'),
        ('false_match', 'False Match'),
        ('inconclusive', 'Inconclusive'),
        ('duplicate', 'Duplicate'),
        ('irrelevant', 'Irrelevant'),
    ], string='Disposition', tracking=True)

    recommendation = fields.Selection([
        ('edd', 'Recommend EDD'),
        ('restrict', 'Recommend Account Restriction'),
        ('exit', 'Recommend Customer Exit'),
        ('monitor', 'Continue Monitoring'),
        ('no_action', 'No Action Required'),
    ], string='Recommendation', tracking=True)

    assigned_officer_id = fields.Many2one('res.users', string='Assigned Officer', tracking=True, index=True)
    review_deadline = fields.Datetime(string='Review Deadline', tracking=True)
    review_notes = fields.Text(string='Review Notes', tracking=True)
    days_open = fields.Integer(string='Days Open', compute='_compute_days_open')

    source_date = fields.Date(string='Publication Date')
    status = fields.Selection([
        ('new', 'New'),
        ('under_review', 'Under Review'),
        ('confirmed', 'Confirmed Risk'),
        ('closed', 'Closed')
    ], string='Status', default='new', tracking=True, inverse='_inverse_status')
    risk_score = fields.Float(string='Risk Score', default=1.0, index=True)

    matched_keyword_ids = fields.Many2many(
        'media.keyword',
        'adverse_media_alert_keyword_rel',
        'alert_id',
        'keyword_id',
        string='Matched Keywords',
    )

    risk_score_decoration = fields.Char(compute='_compute_risk_score_decoration')

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')

    _sql_constraints = [
        ('name_uniq', 'unique (media_id, name)',
         "Alert title must be unique for each media configuration!"),
        ('hash_uniq', 'unique (media_id, content_hash)',
         "A duplicate article was detected for this screening record."),
    ]

    @api.depends('risk_score')
    def _compute_risk_score_decoration(self):
        for record in self:
            if record.risk_score <= float(
                    self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
                record.risk_score_decoration = 'success'
            elif record.risk_score <= float(
                    self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
                record.risk_score_decoration = 'warning'
            else:
                record.risk_score_decoration = 'danger'

    @api.depends('create_date')
    def _compute_days_open(self):
        now = fields.Datetime.now()
        for record in self:
            if record.create_date:
                record.days_open = (now - record.create_date).days
            else:
                record.days_open = 0

    @api.onchange('article_type')
    def _onchange_article_type(self):
        if self.article_type and self.base_risk_score:
            multiplier = _ARTICLE_TYPE_MULTIPLIERS.get(self.article_type, 1.0)
            max_threshold = float(
                self.env['res.compliance.settings'].get_setting('maximum_risk_threshold') or 9
            )
            self.risk_score = min(self.base_risk_score * multiplier, max_threshold)

    def _inverse_status(self):
        for record in self:
            if record.status == 'under_review' and not record.assigned_officer_id:
                raise ValidationError(
                    "An officer must be assigned before moving the alert to Under Review.")
            if record.status in ('confirmed', 'closed') and not record.review_notes:
                raise ValidationError(
                    "Review notes are required before confirming or closing an alert.")
            _logger.info(f"Status changed to '{record.status}' for alert {record.id}")
            if record.status == 'confirmed':
                self.update_partner_risk()

    def update_partner_risk(self):
        """Update the partner's risk score when an alert is confirmed."""
        _logger.info(f"update_partner_risk called for records: {self}")
        for record in self:
            if record.status == 'confirmed' and record.media_id and record.media_id.partner_id:
                partner = record.media_id.partner_id
                max_threshold = float(
                    self.env['res.compliance.settings'].get_setting('maximum_risk_threshold'))

                composite = partner.composite_risk_score or 0
                new_score = composite + record.risk_score if composite > 0 else record.risk_score
                new_score = min(new_score, max_threshold)
                new_level = self.env['res.partner'].compute_customer_rating(new_score)

                _logger.info(
                    f"Updating partner {partner.name}: composite={composite}, "
                    f"alert_score={record.risk_score}, new_score={new_score}, new_level={new_level}")

                self.env.cr.execute(
                    """UPDATE res_partner SET risk_score = %s, risk_level = %s WHERE id = %s""",
                    (new_score, new_level, partner.id)
                )
                partner.invalidate_recordset(['risk_score', 'risk_level'])
                _logger.info(f"Partner {partner.name} risk updated successfully")

    def action_reopen(self):
        for record in self:
            if record.status != 'closed':
                raise UserError("Only closed alerts can be reopened.")
            record.write({'status': 'under_review'})
            record.message_post(body=f"Alert reopened for further review by {self.env.user.name}.")

    def action_mark_false_positive(self):
        self.ensure_one()
        if not self.review_notes:
            raise UserError(
                "Add review notes explaining the false positive decision before marking.")
        expiry_days = int(
            self.env['res.compliance.settings'].get_setting('am_suppression_expiry_days') or 90
        )
        url_domain = ''
        if self.source_url and '//' in self.source_url:
            parts = self.source_url.split('/')
            url_domain = parts[2] if len(parts) > 2 else ''
        self.env['adverse.media.suppression'].create({
            'partner_id': self.partner_id.id,
            'url_domain': url_domain,
            'reason': f"False positive dismissal of alert: {self.name}",
            'created_by': self.env.user.id,
            'expires_on': fields.Date.today() + timedelta(days=expiry_days),
        })
        self.write({'disposition': 'false_match', 'status': 'closed'})
        self.message_post(
            body=f"Marked as false positive by {self.env.user.name}. Suppression rule created for {expiry_days} days.")

    def action_open_case(self):
        """Open a pre-filled Case Management form from this alert."""
        self.ensure_one()
        if 'case.manager' not in self.env:
            raise UserError(
                "The Case Management module is not installed. "
                "Install it to open cases directly from adverse media alerts."
            )
        priority_to_rating = {'0': 'high', '1': 'high', '2': 'medium', '3': 'low', '4': 'low'}
        priority_label = dict(self._fields['priority'].selection).get(self.priority or '2', 'Medium')
        desc_parts = [
            f"Adverse Media Alert: {self.name}",
            f"Partner: {self.partner_id.name or 'N/A'}",
            f"Source: {self.source_name or 'N/A'}",
            f"Publication Date: {self.source_date or 'N/A'}",
            f"Risk Score: {self.risk_score:.2f}",
            f"Priority: {priority_label}",
        ]
        if self.source_url:
            desc_parts.append(f"URL: {self.source_url}")
        if self.description:
            desc_parts.append(f"\nSummary:\n{self.description}")
        return {
            'type': 'ir.actions.act_window',
            'name': 'New Case',
            'res_model': 'case.manager',
            'view_mode': 'form',
            'target': 'current',
            'context': {
                'default_customer_id': self.partner_id.id,
                'default_description': '\n'.join(desc_parts),
                'default_narration': self.name,
                'default_event_date': fields.Datetime.now(),
                'default_case_rating': priority_to_rating.get(self.priority or '2', 'medium'),
                'default_cases_action': 'Review adverse media alert and determine appropriate compliance action.',
            },
        }

    def action_refer_to_str(self):
        """Open a new Suspicious Transaction Report pre-filled from this alert."""
        self.ensure_one()
        if 'nfiu.report' not in self.env:
            raise UserError("The NFIU Reporting module is not installed.")
        return {
            'type': 'ir.actions.act_window',
            'name': 'New Suspicious Transaction Report',
            'res_model': 'nfiu.report',
            'view_mode': 'form',
            'target': 'current',
            'context': {
                'default_name': f"STR — {self.partner_id.name} — {self.name}",
                'default_report_code': 'STR',
                'default_submission_code': 'E',
            },
        }

    @api.model
    def archive_old_alerts(self):
        """Cron: deactivate closed alerts beyond the configured retention period."""
        retention_days = int(
            self.env['res.compliance.settings'].get_setting('am_retention_days') or 2555
        )
        cutoff = fields.Datetime.now() - timedelta(days=retention_days)
        old_alerts = self.search([
            ('create_date', '<', cutoff),
            ('active', '=', True),
            ('status', '=', 'closed'),
        ])
        old_alerts.write({'active': False})
        _logger.info(
            f"Retention cron: archived {len(old_alerts)} closed alert(s) older than "
            f"{retention_days} days."
        )

    @api.model
    def send_weekly_digest(self):
        """Cron: email adverse media managers a weekly summary of alert activity."""
        manager_group = self.env.ref(
            'compliance_management.group_adverse_media_manager',
            raise_if_not_found=False,
        )
        if not manager_group:
            _logger.warning("Weekly digest: group_adverse_media_manager not found — skipping.")
            return

        users = manager_group.users.filtered(lambda u: u.email and u.active)
        if not users:
            return

        now = fields.Datetime.now()
        week_ago = now - timedelta(days=7)

        new_count = self.search_count([('create_date', '>=', week_ago), ('status', '=', 'new')])
        open_count = self.search_count([('status', 'in', ['new', 'under_review'])])
        overdue_count = self.search_count([
            ('review_deadline', '<', now),
            ('review_deadline', '!=', False),
            ('status', 'not in', ['confirmed', 'closed']),
        ])
        confirmed_count = self.search_count([
            ('create_date', '>=', week_ago),
            ('status', '=', 'confirmed'),
        ])
        high_priority_count = self.search_count([
            ('status', 'not in', ['closed']),
            ('priority', 'in', ['0', '1']),
        ])

        week_label = f"{week_ago.strftime('%d %b')} – {now.strftime('%d %b %Y')}"
        red = '#dc3545'
        green = '#28a745'
        blue = '#1a3c5e'

        body = f"""
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
  <div style="background:{blue};color:white;padding:20px 24px;border-radius:4px 4px 0 0;">
    <h2 style="margin:0;font-size:18px;">Adverse Media — Weekly Digest</h2>
    <p style="margin:4px 0 0;font-size:13px;opacity:.8;">{week_label}</p>
  </div>
  <div style="background:#f8f9fa;padding:24px;border:1px solid #dee2e6;border-top:none;border-radius:0 0 4px 4px;">
    <table style="width:100%;border-collapse:collapse;">
      <tr style="background:white;border-bottom:1px solid #dee2e6;">
        <td style="padding:12px 16px;font-size:14px;color:#495057;">New alerts this week</td>
        <td style="padding:12px 16px;text-align:right;font-weight:bold;font-size:16px;color:{blue};">{new_count}</td>
      </tr>
      <tr style="border-bottom:1px solid #dee2e6;">
        <td style="padding:12px 16px;font-size:14px;color:#495057;">Total open alerts</td>
        <td style="padding:12px 16px;text-align:right;font-weight:bold;font-size:16px;color:{blue};">{open_count}</td>
      </tr>
      <tr style="background:white;border-bottom:1px solid #dee2e6;">
        <td style="padding:12px 16px;font-size:14px;color:#495057;">Critical / High priority (open)</td>
        <td style="padding:12px 16px;text-align:right;font-weight:bold;font-size:16px;color:{red if high_priority_count else green};">{high_priority_count}</td>
      </tr>
      <tr style="border-bottom:1px solid #dee2e6;">
        <td style="padding:12px 16px;font-size:14px;color:#495057;">SLA overdue alerts</td>
        <td style="padding:12px 16px;text-align:right;font-weight:bold;font-size:16px;color:{red if overdue_count else green};">{overdue_count}</td>
      </tr>
      <tr style="background:white;">
        <td style="padding:12px 16px;font-size:14px;color:#495057;">Confirmed this week</td>
        <td style="padding:12px 16px;text-align:right;font-weight:bold;font-size:16px;color:{green};">{confirmed_count}</td>
      </tr>
    </table>
    <p style="margin-top:20px;font-size:12px;color:#6c757d;">
      Automated digest from iComply Adverse Media Monitoring. Log in to review open alerts.
    </p>
  </div>
</div>"""

        for user in users:
            self.env['mail.mail'].sudo().create({
                'subject': f'Adverse Media Weekly Digest — {now.strftime("%d %b %Y")}',
                'email_to': user.email,
                'body_html': body,
                'auto_delete': True,
            }).send()

        _logger.info(f"Weekly adverse media digest sent to {len(users)} manager(s).")

    @api.model
    def check_sla_breaches(self):
        """Cron: post an activity on overdue alerts whose review deadline has passed."""
        now = fields.Datetime.now()
        overdue = self.search([
            ('review_deadline', '<', now),
            ('review_deadline', '!=', False),
            ('status', 'not in', ['confirmed', 'closed']),
        ])
        for alert in overdue:
            user_id = alert.assigned_officer_id.id if alert.assigned_officer_id else self.env.user.id
            priority_label = dict(self._fields['priority'].selection).get(alert.priority, alert.priority)
            alert.activity_schedule(
                'mail.mail_activity_data_todo',
                date_deadline=fields.Date.today(),
                summary='SLA Breach — Adverse Media Review Overdue',
                user_id=user_id,
                note=(
                    f'Alert "<b>{alert.name}</b>" for partner <b>{alert.partner_id.name}</b> '
                    f'exceeded its review deadline ({alert.review_deadline}). '
                    f'Priority: {priority_label}. Immediate action required.'
                ),
            )
        _logger.info(f"SLA breach check: {len(overdue)} overdue alert(s) notified.")


class MediaSource(models.Model):
    _name = 'media.source'
    _description = 'Adverse Media RSS Feed Source'
    _order = 'sequence, name'

    name = fields.Char(string='Source Name', required=True)
    url = fields.Char(string='Feed URL', required=True)
    source_type = fields.Selection([
        ('rss', 'RSS Feed'),
    ], string='Source Type', default='rss', required=True)
    credibility_score = fields.Float(
        string='Credibility Score', default=1.0,
        help='0–1 weight applied to risk scores from this source. 1.0 = fully trusted.')
    active = fields.Boolean(default=True)
    last_fetched = fields.Datetime(string='Last Fetched', readonly=True)
    sequence = fields.Integer(default=10)
    notes = fields.Text(string='Notes')


class AdverseMediaSuppression(models.Model):
    _name = 'adverse.media.suppression'
    _description = 'Adverse Media Suppression Rule'
    _order = 'create_date desc'

    partner_id = fields.Many2one(
        'res.partner', string='Partner', required=True, ondelete='cascade', index=True)
    url_domain = fields.Char(
        string='URL Domain',
        help='Suppress all articles from this domain for this partner (e.g. bbc.co.uk)')
    keyword_id = fields.Many2one(
        'media.keyword', string='Keyword Category',
        help='Suppress this keyword category for this partner')
    reason = fields.Text(string='Reason', required=True)
    created_by = fields.Many2one(
        'res.users', string='Created By', default=lambda self: self.env.user, readonly=True)
    expires_on = fields.Date(string='Expires On')
    active = fields.Boolean(default=True)

    @api.model
    def expire_suppression_rules(self):
        """Cron: deactivate suppression rules past their expiry date."""
        today = fields.Date.today()
        expired = self.search([('expires_on', '<', today), ('active', '=', True)])
        expired.write({'active': False})
        _logger.info(f"Expired {len(expired)} suppression rule(s).")


class MediaKeyword(models.Model):
    _name = 'media.keyword'
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
        'res.users',
        'adverse_media_officers',
        string="Officer(s) Responsible",
        tracking=True,
    )
    exclusion_terms = fields.Char(
        string='Exclusion Terms',
        help='Comma-separated terms. If any appear in an article, this keyword will not trigger an alert for that article.',
    )

    risk_score_decoration = fields.Char(compute='_compute_risk_score_decoration')
    active = fields.Boolean(default=True, tracking=True)

    @api.depends('risk_score')
    def _compute_risk_score_decoration(self):
        for record in self:
            if record.risk_score <= float(
                    self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
                record.risk_score_decoration = 'success'
            elif record.risk_score <= float(
                    self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
                record.risk_score_decoration = 'warning'
            else:
                record.risk_score_decoration = 'danger'

    @api.onchange('risk_score')
    def _onchange_risk_score(self):
        if self.risk_score <= float(
                self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
            self.media_risk_level = "low"
        elif self.risk_score <= float(
                self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
            self.media_risk_level = "medium"
        else:
            self.media_risk_level = "high"
