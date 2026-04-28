# Repository Guidelines

## Project Structure & Module Organization
This repository is an Odoo 16 deployment with custom modules in `addons/`. Each addon typically follows the standard Odoo layout: `__manifest__.py`, Python packages such as `models/`, `wizards/`, and `controllers/`, UI/data files in `views/`, `security/`, and `data/`, plus frontend assets under `static/src/`. Local runtime configuration lives in `etc/` (`etc/odoo.conf`, `etc/requirements.txt`), persistent database files in `postgresql/`, and screenshots/docs in `screenshots/`.

## Build, Test, and Development Commands
Use Docker Compose for the normal local workflow:

- `docker-compose up -d`: start Odoo and PostgreSQL in the background.
- `docker-compose restart`: reload services after config or dependency changes.
- `docker-compose down`: stop the stack.
- `docker-compose logs -f odoo`: follow server logs while debugging.

For addon testing, run Odoo with a targeted module list and tests enabled, for example:

```bash
odoo-bin -c etc/odoo.conf -d dev_db -i queue_job --test-enable --stop-after-init
```

## Coding Style & Naming Conventions
Follow Odoo/Python conventions: 4-space indentation, `snake_case` for Python methods/files, and descriptive module names such as `hr_employee_transfer`. Keep manifest keys and XML data files explicit and ordered. Name XML/CSV files by feature and purpose, for example `views/queue_job_views.xml` or `security/ir.model.access.csv`. Frontend assets belong under `static/src/`; keep custom libraries out of root module directories.

## Testing Guidelines
Place automated tests inside each addon’s `tests/` package and name files `test_*.py`. Prefer module-scoped tests that validate models, security rules, and wizards close to the code they cover. `pytest` is available in `etc/requirements.txt`, but most validation here is still driven through Odoo’s native test runner with `--test-enable`. Run tests against only the modules you changed to keep feedback fast.

## Commit & Pull Request Guidelines
This checkout has no local commit history, so use a clean imperative style: `queue_job: fix retry channel lookup`. Keep commits scoped to one addon or one operational change. Pull requests should state the affected module(s), describe behavior changes, list test commands run, and include screenshots for UI changes.

## Security & Configuration Tips
Do not commit real credentials or database dumps. Treat `etc/odoo.conf` as environment-specific, and review `admin_passwd`, addon paths, and exposed ports before sharing the repository.

## Agent-Specific Instructions
Treat `seatunnel/` and `.docker/seatunnel/` as maintained integration code, not archive material. For any SeaTunnel task, check the Apache SeaTunnel 2.3.12 documentation first: `https://seatunnel.apache.org/docs/2.3.12/`. Keep configs, scripts, and connector usage aligned with that version unless the repository is explicitly upgraded.
