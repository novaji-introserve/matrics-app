# -*- coding: utf-8 -*-
from odoo import models, fields, api, tools, _
from datetime import timedelta
import logging
import gc
import random



_logger = logging.getLogger(__name__)




class CustomerDigitalProduct(models.Model):
    _name = 'customer.digital.product'

    customer_id = fields.Text(string='Customer ID',
                              index=True, readonly=True)  # customer,
    customer_name = fields.Char(string='Name', readonly=True)
    customer_segment = fields.Char(
        string='Customer Segment', readonly=True)
    ussd = fields.Char(string='Uses USSD', index=True, readonly=True)
    carded_customer = fields.Char(
        string='Has A Card', index=True, readonly=True)
    alt_bank = fields.Char(string='Is On Alt Bank', readonly=True)
    

  
    def init(self):
        # Minimal initialization for fast loading
        self.env.cr.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'customer_digital_product'
            )
        """)
        table_exists = self.env.cr.fetchone()[0]

        if table_exists:
            # Only create essential index
            try:
                self.env.cr.execute("""
                    CREATE INDEX IF NOT EXISTS customer_digital_product_customer_id_idx
                    ON customer_digital_product (customer_id)
                """)
            except Exception as e:
                _logger.warning(f"Index creation skipped: {e}")


   
class DigitalDeliveryChannel(models.Model):
    """Optimized model for handling millions of records"""
    _name = 'digital.delivery.channel'
    _description = 'Digital Delivery Channel'
    _sql_constraints = [
        ('uniq_channel_code', 'unique(code)',
         "Channel code already exists. Code must be unique!"),
    ]
    _order = "name"

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True, index=True,
                       help="Technical code for the channel (e.g., 'ussd', 'onebank')")
    description = fields.Text(string="Description")
    status = fields.Selection(string='Status', selection=[
        ('active', 'Active'),
        ('inactive', 'Inactive')
    ], default='active', index=True)
    


class CustomerChannelSubscription(models.Model):
    """Optimized subscription model for large datasets"""
    _name = 'customer.channel.subscription'
    _description = 'Customer Channel Subscription'
    _sql_constraints = [
        ('uniq_customer_channel', 'unique(customer_id, channel_id)',
         "This customer already has this channel registered!"),
    ]

    customer_id = fields.Char(string='Customer ID', required=True, index=True)
    partner_id = fields.Many2one('res.partner', string='Partner',
                                 index=True, readonly=False)
    channel_id = fields.Many2one('digital.delivery.channel', string='Channel',
                                 required=True, index=True, ondelete='restrict')
    value = fields.Char(string='Value', index=True,
                        help="The value/status of this channel for the customer")

    subscription_date = fields.Date(string='Subscription Date')
    last_updated = fields.Datetime(
        string='Last Updated', default=fields.Datetime.now, index=True)


    @api.model_create_multi
    def create(self, vals_list):
        """Optimized batch create for large datasets"""
        # Set last_updated for all records
        for vals in vals_list:
            vals['last_updated'] = fields.Datetime.now()
        return super().create(vals_list)

    def write(self, vals):
        """Optimized write with minimal overhead"""
        vals['last_updated'] = fields.Datetime.now()
        return super().write(vals)

   
class CustomerDigitalProductMaterialized(models.Model):
    """Materialized view for efficient delivery channel lookups"""
    _name = 'customer.digital.product.mat'
    _description = 'Customer Digital Products Materialized'
    _auto = False  # This is not a regular table

    id = fields.Integer(readonly=True)
    customer_id = fields.Char(string='Customer ID', readonly=True, index=True)
    customer_name = fields.Char(string='Name', readonly=True)
    customer_segment = fields.Char(string='Customer Segment', readonly=True)
    ussd = fields.Char(string='Uses USSD', readonly=True)
    carded_customer = fields.Char(string='Has A Card', readonly=True)
    alt_bank = fields.Char(string='Is On Alt Bank', readonly=True)
    

    def init(self):
        """Minimal initialization - no heavy operations"""
        _logger.info("Materialized view model initialized (deferred creation)")

    @api.model
    def refresh_view(self):
        """Safe refresh method"""
        try:
            self._cr.execute(
                "REFRESH MATERIALIZED VIEW customer_digital_product_mat")
            return True
        except Exception as e:
            _logger.error(f"Failed to refresh materialized view: {e}")
            return False
        