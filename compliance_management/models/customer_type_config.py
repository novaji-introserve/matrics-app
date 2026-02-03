# -*- coding: utf-8 -*-
"""
Customer Type Configuration Model
==================================

This module provides a configuration table for classifying customer status values
as either 'Individual' or 'Corporate'. This configuration allows flexible classification
without hardcoding business rules.

Design Rationale:
-----------------
Following SOLID principles and industry best practices:

    1. Single Responsibility: This model ONLY handles customer type classification configuration

    2. Open/Closed Principle: Easy to extend with new status values via configuration,
       without modifying code

    3. Separation of Concerns: Classification rules are data (configuration),
       not code (business logic)

    4. Maintainability: Bank staff can update classification rules via UI
       without developer intervention

Business Rules:
---------------
Based on client requirements, the default classification is:
    - Individual: "AltEvolve", "Internal Customer", "Laborer", "Individual"
    - Corporate: All other status values

However, this can be customized per bank via the configuration interface.

Usage:
------
    # Get customer type for a status value
    config = env['customer.type.config'].get_customer_type('AltEvolve')
    # Returns: 'individual'

    # Check if a status is individual
    is_individual = env['customer.type.config'].is_individual_status('Corporate')
    # Returns: False

Author: Olumide Awodeji
Version: 1.0.0
"""

from odoo import api, fields, models, _
from odoo.exceptions import ValidationError
import logging

_logger = logging.getLogger(__name__)


class CustomerTypeConfig(models.Model):
    """
    Customer Type Configuration

    Stores classification rules for determining whether a customer_status
    value represents an Individual or Corporate customer type.

    Attributes:
        customer_status (Char): The customer status value (must match customer.status.customer_status)
        customer_type (Selection): Classification as 'individual' or 'corporate'
        description (Text): Optional notes about this classification
        active (Boolean): Whether this configuration is active
    """

    _name = 'customer.type.config'
    _description = 'Customer Type Classification Configuration'
    _order = 'customer_status'

    # -------------------------------------------------------------------------
    # SQL Constraints
    # -------------------------------------------------------------------------
    _sql_constraints = [
        (
            'unique_customer_status',
            'UNIQUE(customer_status)',
            'Customer status classification already exists. Each status can only have one classification.'
        ),
    ]

    # -------------------------------------------------------------------------
    # Field Definitions
    # -------------------------------------------------------------------------

    customer_status = fields.Char(
        string='Customer Status',
        required=True,
        index=True,
        help='The customer status value to classify (e.g., "Individual", "Corporate", "AltEvolve")'
    )

    customer_type = fields.Selection(
        selection=[
            ('individual', 'Individual'),
            ('corporate', 'Corporate')
        ],
        string='Customer Type',
        required=True,
        default='corporate',
        index=True,
        help='Classification: Individual or Corporate. '
             'Determines which name to display in enrichment data.'
    )

    description = fields.Text(
        string='Description',
        help='Optional notes about this classification rule'
    )

    active = fields.Boolean(
        string='Active',
        default=True,
        help='Inactive rules will not be used for classification'
    )

    # Make the name field required for Odoo's name_get
    name = fields.Char(
        string='Configuration Name',
        compute='_compute_name',
        store=True
    )

    # -------------------------------------------------------------------------
    # Computed Fields
    # -------------------------------------------------------------------------

    @api.depends('customer_status', 'customer_type')
    def _compute_name(self):
        """Compute display name from customer_status and type."""
        for record in self:
            if record.customer_status and record.customer_type:
                record.name = f"{record.customer_status} ({record.customer_type.title()})"
            else:
                record.name = record.customer_status or 'New'

    # -------------------------------------------------------------------------
    # Constraints and Validations
    # -------------------------------------------------------------------------

    @api.constrains('customer_status')
    def _check_customer_status_not_empty(self):
        """Ensure customer_status is not empty or whitespace only."""
        for record in self:
            if not record.customer_status or not record.customer_status.strip():
                raise ValidationError(
                    _('Customer Status cannot be empty or contain only whitespace.')
                )

    # -------------------------------------------------------------------------
    # Public API Methods
    # -------------------------------------------------------------------------

    @api.model
    def get_customer_type(self, customer_status_value):
        """
        Get the customer type for a given customer status value.

        Args:
            customer_status_value (str): The customer status to classify

        Returns:
            str: 'individual' or 'corporate' (defaults to 'corporate' if not configured)

        Example:
            customer_type = self.env['customer.type.config'].get_customer_type('AltEvolve')
            # Returns: 'individual'
        """
        if not customer_status_value:
            return 'corporate'  # Default for empty/None values

        config = self.search([
            ('customer_status', '=', customer_status_value),
            ('active', '=', True)
        ], limit=1)

        if config:
            return config.customer_type
        else:
            # Default to corporate if no configuration exists
            _logger.debug(
                f'No classification config found for status "{customer_status_value}", '
                f'defaulting to corporate'
            )
            return 'corporate'

    @api.model
    def is_individual_status(self, customer_status_value):
        """
        Check if a customer status value is classified as individual.

        Args:
            customer_status_value (str): The customer status to check

        Returns:
            bool: True if individual, False if corporate

        Example:
            is_indiv = self.env['customer.type.config'].is_individual_status('Individual')
            # Returns: True
        """
        return self.get_customer_type(customer_status_value) == 'individual'

    @api.model
    def bulk_classify(self, customer_status_values):
        """
        Classify multiple customer status values at once.

        Args:
            customer_status_values (list): List of customer status values

        Returns:
            dict: Mapping of customer_status -> customer_type

        Example:
            classifications = self.env['customer.type.config'].bulk_classify([
                'Individual', 'Corporate', 'AltEvolve'
            ])
            # Returns: {'Individual': 'individual', 'Corporate': 'corporate', ...}
        """
        if not customer_status_values:
            return {}

        # Fetch all configurations for the given status values
        configs = self.search([
            ('customer_status', 'in', customer_status_values),
            ('active', '=', True)
        ])

        # Build mapping
        result = {}
        for status_value in customer_status_values:
            config = configs.filtered(lambda c: c.customer_status == status_value)
            result[status_value] = config.customer_type if config else 'corporate'

        return result
