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
        # --- Index 1: pep_list_name_normalized_idx ---
        self.env.cr.execute("""
            SELECT 1 
            FROM pg_indexes 
            WHERE indexname = 'pep_list_name_normalized_idx'
        """)
        if not self.env.cr.fetchone():
            self.env.cr.execute("""
                CREATE INDEX pep_list_name_normalized_idx 
                ON pep_list (LOWER(REPLACE(name, ' ', '')));
            """)
        
        # --- Index 2: pep_list_firstname_lastname_idx ---
        self.env.cr.execute("""
            SELECT 1 
            FROM pg_indexes 
            WHERE indexname = 'pep_list_firstname_lastname_idx'
        """)
        if not self.env.cr.fetchone():
            self.env.cr.execute("""
                CREATE INDEX pep_list_firstname_lastname_idx 
                ON pep_list (LOWER(REPLACE(firstname, ' ', '')), LOWER(REPLACE(lastname, ' ', '')));
            """)
        
        # --- Index 3: pep_list_id_idx ---
        self.env.cr.execute("""
            SELECT 1 
            FROM pg_indexes 
            WHERE indexname = 'pep_list_id_idx'
        """)
        if not self.env.cr.fetchone():
            self.env.cr.execute(
                "CREATE INDEX pep_list_id_idx ON pep_list (id)"
            )
        