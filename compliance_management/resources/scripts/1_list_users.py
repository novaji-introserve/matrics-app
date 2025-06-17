#!/usr/bin/env python
# python -m click_odoo -d icomply_dev -c /home/jonathan/etc/odoo.conf  /home/jonathan/Projects/icomply_odoo/compliance_management/resources/scripts/list-users.py
from __future__ import print_function

for u in env['res.users'].search([]):
    print(u.login, u.name)