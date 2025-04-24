# -*- coding: utf-8 -*-
{
    'name': 'iComply ETL Manager',
    'version': '2.0',
    'category': 'icomply',
    'summary': 'Enterprise-grade ETL solution for seamless data integration across databases',
    'description': """
iComply ETL Manager
==================
A powerful Extract, Transform, Load (ETL) solution for Odoo that enables seamless data integration across multiple database systems.

Key Features:
------------
* Connect to multiple database types (PostgreSQL, MySQL, MSSQL, Oracle, SQLite, etc.)
* Configure table mappings with column transformations
* Schedule automatic data synchronization
* Monitor ETL processes with detailed dashboards
* Support for large dataset processing with automatic chunking
* High-performance batch operations optimized for each database type
* Easy-to-use interface for managing all your ETL processes

Technical Benefits:
-----------------
* Optimized connection management to prevent memory leaks
* Efficient batch operations with proper UPSERT support for all databases
* Smart chunking for handling large tables (millions of records)
* Detailed logging and performance monitoring
* Thread-safe implementation for concurrent processing
    """,
    'author': 'Hanson Eyuren & Olumide Awodeji (Synth corp)',
    'website': 'https://www.cybercraftsmen.tech',
    'depends': ['base', 'mail', 'queue_job'],
    'data': [
        'security/ir.model.access.csv',
        'data/preloaded_data/etl_default_data.xml',
        'data/preloaded_data/etl_database_types.xml',
        'views/etl_database_views.xml',
        'views/etl_sync_log_views.xml',
        'views/etl_config_views.xml',
        'views/etl_dashboard.xml',
        'views/etl_menu.xml',
        'data/schedules/etl_cron.xml',
        'data/preloaded_data/etl_data.xml',
    ],
    'installable': True,
    'application': True,
    'auto_install': False,
    'license': 'LGPL-3',
    'images': ['static/description/icon.png'],
    'price': 199.99,
    'currency': 'EUR',
    'maintainer': 'Synth Corporation',
    'support': 'olumide.awodeji@hotmail.com',
    'demo': [],
    # 'external_dependencies': {
    #     'python': ['psycopg2', 'mysql-connector-python', 'pyodbc', 'cx_Oracle', 'sqlite3'],
    # },
}