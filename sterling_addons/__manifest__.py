{
    'name': 'Demo Data For Sterling Bank',
    'version': '1.0.0',
    'category': 'iComply',
    'summary': 'Demo data and extensions for Compliance Management (Sterling Bank)',
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
         
         "demo/sterling/digital_channels/channels.xml",
        
        # Risk Subjects
        "demo/sterling/subjects/account_product.xml",
        "demo/sterling/subjects/customer_industry.xml",
        "demo/sterling/subjects/delivery_subject.xml",
        "demo/sterling/subjects/region_subject.xml",
        
        # Risk Assessments
        "demo/sterling/risk_assessment/jurisdiction_as.xml",
        "demo/sterling/risk_assessment/account_product_assessments.xml",
        "demo/sterling/risk_assessment/customer_industry_assessments.xml",
        "demo/sterling/risk_assessment/delivery_channels_assessments.xml",
       
        # Risk Plans
        "demo/sterling/risk_plan/account_product_plans.xml",
        "demo/sterling/risk_plan/customer_industry_plans.xml",
        "demo/sterling/risk_plan/jurisdiction_plan.xml",
        "demo/sterling/risk_plan/delivery_channels_analysis.xml",
       
    ],
   "assets": {
    "web.assets_backend": [],
},
    'installable': True,
    'application': False,
    'auto_install': False,
    'license': 'LGPL-3',
}
