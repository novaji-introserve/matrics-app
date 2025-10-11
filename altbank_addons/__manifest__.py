{
    'name': 'Demo Data For AltBank',
    'version': '1.0.0',
    'category': 'iComply',
    'summary': 'Demo data and extensions for Compliance Management (AltBank)',
    'description': """
            This module provides:
            - Sample data for compliance management
            - Extensions to existing compliance models
        """,
    'author': 'Novaji Introserve',
    'depends': [
        'base',
        'web',
        "compliance_management"
    ],
    'data': [
         
         "demo/altbank/digital_channels/channels.xml",
        
        # Risk Subjects
        "demo/altbank/subjects/account_product.xml",
        "demo/altbank/subjects/customer_industry.xml",
        "demo/altbank/subjects/delivery_subject.xml",
        "demo/altbank/subjects/region_subject.xml",
        
        # Risk Assessments
        "demo/altbank/risk_assessment/jurisdiction_as.xml",
        "demo/altbank/risk_assessment/account_product_assessments.xml",
        "demo/altbank/risk_assessment/customer_industry_assessments.xml",
        "demo/altbank/risk_assessment/delivery_channels_assessments.xml",
       
        # Risk Plans
        "demo/altbank/risk_plan/account_product_plans.xml",
        "demo/altbank/risk_plan/customer_industry_plans.xml",
        "demo/altbank/risk_plan/jurisdiction_plan.xml",
        "demo/altbank/risk_plan/delivery_channels_analysis.xml",
       
    ],
   "assets": {
    "web.assets_backend": [],
},
    'installable': True,
    'application': False,
    'auto_install': False,
    'license': 'LGPL-3',
}
