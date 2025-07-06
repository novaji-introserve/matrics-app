from odoo import models, fields, api



class ExceptionCategory(models.Model):
    _name = 'exception.category.'
    _description = 'Exception Category'
    _inherit = ['mail.thread', 'mail.activity.mixin']


    num_id = fields.Integer(string='ID')
    name = fields.Char(string='Name', required=True, tracking=True, index=True)
    description = fields.Char(string='Description', tracking=True)
    
    def init(self):
        indexes = [
            {
                'name': 'exception_category_id_idx',
                'column': 'id',
                'query': "CREATE INDEX exception_category_id_idx ON exception_category_(id)"
            }
        ]

        # Create indexes only if they don't exist
        for index in indexes:
            self._cr.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE schemaname = 'public' 
                    AND tablename = 'exception_category_'
                    AND indexname = %s
                )
            """, (index['name'],))
            index_exists = self._cr.fetchone()[0]

            if not index_exists:
                self._cr.execute(index['query'])


class ExceptionProcess(models.Model):
    _name = 'exception.process.'
    _description = 'Exception Process'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Name', required=True, tracking=True)
    type_id = fields.Many2one('exception.category.',
                              string='Process Category', required=True, tracking=True,index=True)
    description = fields.Char(string='Description', tracking=True)
    
    def init(self):
        indexes = [
            {
                'name': 'exception_process_id_idx',
                'column': 'id',
                'query': "CREATE INDEX exception_process_id_idx ON exception_process_(id)"
            }
        ]

        # Create indexes only if they don't exist
        for index in indexes:
            self._cr.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE schemaname = 'public' 
                    AND tablename = 'exception_process_'
                    AND indexname = %s
                )
            """, (index['name'],))
            index_exists = self._cr.fetchone()[0]

            if not index_exists:
                self._cr.execute(index['query'])
