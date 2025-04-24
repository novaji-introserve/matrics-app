from odoo import api, SUPERUSER_ID

def post_init_hook(cr, registry):
    """Set up queue job configuration after module installation"""
    with api.Environment.manage():
        env = api.Environment(cr, SUPERUSER_ID, {})
        
        # Create channel
        channel = env['queue.job.channel'].search([('name', '=', 'ETL')], limit=1)
        if not channel:
            channel = env['queue.job.channel'].create({
                'name': 'ETL',
                'complete_name': 'root.ETL',
            })
        
        # Get model reference
        model_id = env['ir.model'].search([('model', '=', 'etl.fast.sync.postgres')], limit=1).id
        
        # Get reference to retry pattern and related action
        retry_pattern_id = env.ref('queue_job.retry_pattern_job_error').id
        related_action_id = env.ref('queue_job.related_action_store_id').id
        
        # Create job functions
        job_functions = []
        
        # Extract function
        extract_job = env['queue.job.function'].search([
            ('model_id', '=', model_id),
            ('method', '=', '_extract_batch_to_csv')
        ], limit=1)
        
        if not extract_job:
            extract_job = env['queue.job.function'].create({
                'model_id': model_id,
                'method': '_extract_batch_to_csv',
                'channel_id': channel.id,
                'retry_pattern_id': retry_pattern_id,
                'related_action_id': related_action_id,
            })
            job_functions.append(extract_job.id)
        
        # Coordinator function
        coord_job = env['queue.job.function'].search([
            ('model_id', '=', model_id),
            ('method', '=', '_monitor_extraction_progress')
        ], limit=1)
        
        if not coord_job:
            coord_job = env['queue.job.function'].create({
                'model_id': model_id,
                'method': '_monitor_extraction_progress',
                'channel_id': channel.id,
                'retry_pattern_id': retry_pattern_id,
                'related_action_id': related_action_id,
            })
            job_functions.append(coord_job.id)
        
        # Load function
        load_job = env['queue.job.function'].search([
            ('model_id', '=', model_id),
            ('method', '=', '_load_csv_file_to_db')
        ], limit=1)
        
        if not load_job:
            load_job = env['queue.job.function'].create({
                'model_id': model_id,
                'method': '_load_csv_file_to_db',
                'channel_id': channel.id,
                'retry_pattern_id': retry_pattern_id,
                'related_action_id': related_action_id,
            })
            job_functions.append(load_job.id)
        
        # Update channel with job functions
        if job_functions:
            channel.write({
                'job_function_ids': [(4, job_id) for job_id in job_functions]
            })
        
        # Create cron job for checking stuck jobs
        cron_job = env['ir.cron'].search([('name', '=', 'Check Stuck ETL Jobs')], limit=1)
        if not cron_job:
            cron_job = env['ir.cron'].create({
                'name': 'Check Stuck ETL Jobs',
                'model_id': env['ir.model'].search([('model', '=', 'etl.fast.sync.postgres')], limit=1).id,
                'state': 'code',
                'code': 'model._check_stuck_jobs()',
                'interval_number': 5,
                'interval_type': 'minutes',
                'numbercall': -1,
                'active': True,
            })
