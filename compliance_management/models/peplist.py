from odoo import _, api, fields, models, tools
import logging
import uuid

_logger = logging.getLogger(__name__)

class PepCustomer(models.Model):
    _name = 'pep.list'
    _description = 'PEP List'
    _sql_constraints = [
        ('uniq_unique_id', 'unique(unique_id)',
         "PEP already exists. PEP must be unique!"),
    ]

  
    firstname = fields.Char(string='Firstname', tracking=True)
    lastname = fields.Char(string='Lastname', tracking=True)
    name = fields.Char(string='Name', tracking=True)
    unique_id = fields.Char(string='Unique Identifier', tracking=True,
                            default=lambda self: str(uuid.uuid4()), readonly=True,  copy=False)
    position = fields.Text(string='Position', tracking=True)
    
    def init(self):
        # Add performance-critical indexes for large datasets
        self.env.cr.execute("""
            CREATE INDEX IF NOT EXISTS pep_list_name_normalized_idx 
            ON pep_list (LOWER(REPLACE(name, ' ', '')));
            
            CREATE INDEX IF NOT EXISTS pep_list_firstname_lastname_idx 
            ON pep_list (LOWER(REPLACE(firstname, ' ', '')), LOWER(REPLACE(lastname, ' ', '')));          
           
        """)
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS pep_list_id_idx ON pep_list (id)")
