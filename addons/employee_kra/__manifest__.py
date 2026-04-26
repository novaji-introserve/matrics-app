# -*- coding: utf-8 -*-
#╔══════════════════════════════════════════════════════════════════════╗
#║                                                                      ║
#║                  ╔═══╦╗       ╔╗  ╔╗     ╔═══╦═══╗                   ║
#║                  ║╔═╗║║       ║║ ╔╝╚╗    ║╔═╗║╔═╗║                   ║
#║                  ║║ ║║║╔╗╔╦╦══╣╚═╬╗╔╬╗ ╔╗║║ ╚╣╚══╗                   ║
#║                  ║╚═╝║║║╚╝╠╣╔╗║╔╗║║║║║ ║║║║ ╔╬══╗║                   ║
#║                  ║╔═╗║╚╣║║║║╚╝║║║║║╚╣╚═╝║║╚═╝║╚═╝║                   ║
#║                  ╚╝ ╚╩═╩╩╩╩╩═╗╠╝╚╝╚═╩═╗╔╝╚═══╩═══╝                   ║
#║                            ╔═╝║     ╔═╝║                             ║
#║                            ╚══╝     ╚══╝                             ║
#║                  SOFTWARE DEVELOPED AND SUPPORTED BY                 ║
#║                ALMIGHTY CONSULTING SOLUTIONS PVT. LTD.               ║
#║                      COPYRIGHT (C) 2016 - TODAY                      ║
#║                      https://www.almightycs.com                      ║
#║                                                                      ║
#╚══════════════════════════════════════════════════════════════════════╝
{
    'name': 'Employee Performance Evaluation by KRA/Value Rating',
    'version': '1.0.1',
    'category': 'Human Resources',
    'summary': 'Employee Performance Evaluation by KRA(Key Result Area)/KPA(Key Performance Area)',
    'description':"""Emplyee performance evaluation by KRA(Key Result Area)/KPA(Key Performance Area)
    HR Evaluation Employee Evaluation Emplyee Performance Calculation Employee Appraisals
    employee evaluation form employee evaluation comments evaluation parameters employee performance appraisal 
    employee performance evaluation employee review

    Evaluación del rendimiento de Emplyee por KRA (Área de resultados clave) / KPA (Área de rendimiento clave)
     Evaluación de recursos humanos Evaluación de empleados Evaluación de desempeño de empleados Evaluación de empleados
     formulario de evaluación del empleado evaluación de los empleados comentarios parámetros de evaluación evaluación del 
    desempeño del empleado
     evaluación del desempeño del empleado revisión del empleado

    Évaluation de la performance des employés par KRA (domaine de résultats clé) / KPA (domaine de performance clé)
     Évaluation des RH Évaluation des employés Calcul du rendement des employés Évaluation des employés
     formulaire d'évaluation des employés commentaires d'évaluation des employés paramètres d'évaluation évaluation du rendement des employés
     évaluation des employés évaluation des employés

    تقييم أداء Empleee بواسطة KRA (منطقة النتيجة الرئيسية) / KPA (مفتاح الأداء الرئيسي)
     تقييم الموارد البشرية تقييم الموظف حساب أداء الموظفات تقييم الأداء
     تقييم الموظف شكل تقييم الموظف تعليقات تقييم المعلمات تقييم أداء الموظف
     تقييم الموظف لتقييم أداء الموظف
    
    Beurteilung der Mitarbeiterleistung durch KRA (Hauptergebnisbereich) / KPA (Key Performance Area)
     Mitarbeiterbewertung Personalbewertung Mitarbeiterbewertung Mitarbeiterbewertungen
     Mitarbeiterbeurteilungsformular Mitarbeiterbeurteilungskommentare Bewertungsparameter Beurteilung der Mitarbeiterleistung
     Beurteilung der Mitarbeiterleistung
    
    """,
    'author': 'Almighty Consulting Solutions Pvt. Ltd.',
    'depends': ['hr'],
    'website': 'https://www.almightycs.com',
    'live_test_url': 'https://www.youtube.com/watch?v=kNzGIohZy3w',
    'license': 'OPL-1',
    'data': [
        'security/security.xml',
        'security/ir.model.access.csv',
        'data/data.xml', 
        'views/kra_view.xml',
        'views/value_rating_view.xml',
        'report/kra_report.xml',
        'report/value_rating_report.xml',
        'wizard/create_kra_view.xml',
        'views/menu_items.xml',
    ],
    'images': [
        'static/description/hr_evaluation_kra_odoo_cover.jpg',
    ],
    'installable': True,
    'auto_install': False,
    'application': False,
    'price': 31,
    'currency': 'USD',
}

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
