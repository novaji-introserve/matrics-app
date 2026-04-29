# -*- coding: utf-8 -*-
{
    'name': 'ETL Manager - Advanced Multi-Database ETL System',
    'version': '2.0.0',
    'category': 'Data Management',
    'summary': 'Advanced ETL system with multi-database support, connection pooling, and intelligent queue management',
    'description': """
Advanced ETL Manager
====================

A comprehensive ETL (Extract, Transform, Load) system for Odoo with enterprise-grade features:

**Key Features:**
* **Multi-Database Support**: Connect to PostgreSQL, MySQL, MSSQL, Oracle, and more
* **Flexible Architecture**: Each table can have different source and target databases
* **Connection Pooling**: Optimized connection management with auto-scaling
* **Intelligent Queue Management**: Priority-based processing with fairness controls
* **Dual Sync Types**: Full sync and incremental sync with independent scheduling
* **Health Monitoring**: Real-time connection and performance monitoring
* **Async Logging**: High-performance logging system
* **Admin Controls**: Emergency stops, priority promotion, and detailed monitoring

**New in Version 2.0:**
* Complete architecture redesign for flexibility and performance
* Support for distributed database environments
* Advanced queue management with priority levels
* Connection pool optimization with health checks
* Configurable alert system
* Enhanced monitoring and reporting

**Use Cases:**
* Data warehouse synchronization
* Multi-system data integration
* Legacy system migration
* Real-time data replication
* Business intelligence data preparation

**Supported Databases:**
* PostgreSQL (source/target)
* MySQL (source/target) 
* Microsoft SQL Server (source/target)
* Oracle Database (source/target)
* SQLite (source/target)

**Performance Features:**
* Parallel processing
* Intelligent batching
* Connection pooling
* Async operations
* Memory optimization
* Progress tracking

**Administration Features:**
* Web-based configuration
* Real-time monitoring dashboard
* Queue management interface
* Connection testing tools
* Performance metrics
* Alert configuration
    """,
    'author': 'Eyuren Hanson',
    'website': 'hansoneyuren@gmail.com',
    'license': 'LGPL-3',
    'depends': [
        'base',
        'mail'
    ],
    'external_dependencies': {
        'python': [
            'psycopg2-binary',  # PostgreSQL support
            'pyodbc',           # MSSQL support  
            'pymysql',          # MySQL support
            'psutil',           # System monitoring
            'oracledb',      # Oracle support
        ],
    },
    'data': [
        # Security
        'security/ir.model.access.csv',
        
        # Data files
        'data/etl_data.xml',
        'data/etl_default_data.xml',
        
        # View files
        'views/etl_connections_views.xml',
        'views/etl_config_views.xml',
        'views/etl_sync_log_views.xml',
        'views/etl_dashboard.xml',
        'views/etl_menu.xml',
        
        # Cron jobs
        'data/etl_cron.xml',
    ],
    'demo': [
        'demo/etl_demo_data.xml',
    ],
    'qweb': [],
    'images': [
        'static/description/banner.png',
        'static/description/icon.png',
    ],
    'installable': True,
    'auto_install': False,
    'application': True,
    'sequence': 100,
}