#!/usr/bin/env python
from __future__ import print_function
import click
import os
import odoo_connect
from odoo_connect.explore import explore
from dotenv import load_dotenv, dotenv_values 

load_dotenv()


@click.command()
def main():
    """List users in the system."""
    odoo = env = odoo_connect.connect(url=os.getenv("HOST_URL"), database=os.getenv("DB"),username=os.getenv("USERNAME"), password=os.getenv('PASSWORD'))
    user = explore(env['res.users'])
    for u in user.search([]):
        print(f"{u.login}, {u.name}")


if __name__ == '__main__':
    main()