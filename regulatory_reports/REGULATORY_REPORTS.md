# Regulatory Reports Module — User & Developer Guide

## What This Module Does

The Regulatory Reports module lets you generate periodic compliance returns for Nigerian
regulators (CBN, FIRS, CAC, SEC, etc.) by filling in Excel templates with live data from
the database. Instead of manually typing figures into a spreadsheet every month, you:

1. Upload the blank regulator template once
2. Tell the system which cell in the template corresponds to which piece of data
3. Click **Run Report** — the system fills in every cell automatically and saves the output

---

## Concepts (Read This First)

There are five building blocks. Understanding their relationship is key.

```
Report Entity  ──►  Report Template  ──►  Report
                         │
                    Template Item (cell mapping)
                         │
                    Report Item (the data source)
```

| Concept | Plain English |
|---|---|
| **Report Entity** | The regulator you send the report to (CBN, NFIU, SEC …) |
| **Report Item** | A single piece of data — either a fixed value or a SQL query |
| **Report Template** | The blank Excel file from the regulator + its cell-to-data mappings |
| **Template Item** | One mapping: "cell B12 on sheet Summary = Report Item X" |
| **Report** | An actual run: pick a template, set the date range, click Run |

---

## Step-by-Step Setup

### Step 1 — Create a Report Entity (one-time)

> **Menu:** Compliance Configuration → Regulatory → Report Entities

These are pre-loaded (CBN, FIRS, CAC, SEC). Only add a new one if you have a new
regulator not in the list.

Fields:
- **Name** — e.g. `Central Bank of Nigeria`
- **Code** — short identifier, e.g. `CBN`

---

### Step 2 — Create Report Items (data sources)

> **Menu:** Compliance Configuration → Regulatory → Report Items

A Report Item is a reusable data source. You create it once and reuse it across multiple
templates.

**Source types:**

| Source | When to use | What to fill |
|---|---|---|
| **Static** | Fixed text or number that never changes | Put the value in **Source Value** |
| **SQL Single** | One number pulled from the DB (e.g. total deposits) | Write a `SELECT` in **Source SQL** that returns one row, one column |
| **SQL Multi** | A table of rows (e.g. list of large transactions) | Write a `SELECT` that returns multiple rows |

**Example — static item:**
```
Name:         Report Period Label
Code:         RPT_PERIOD
Source:       Static
Source Value: Monthly Return
```

**Example — SQL single item (total deposits this month):**
```
Name:        Total Deposits
Code:        TOTAL_DEPOSITS
Source:      SQL Query returning single value
Source SQL:
  SELECT SUM(amount)
  FROM res_customer_transaction
  WHERE state = 'done'
    AND date_created >= (date_trunc('month', now()))
    AND date_created <  (date_trunc('month', now()) + interval '1 month')
```

> **Tip:** Use the **Validate SQL Query** button in the form header to check your SQL
> before saving. The query must be a `SELECT` — never an `INSERT`, `UPDATE`, or `DELETE`.

**Example — SQL multi item (large transactions list):**
```
Name:        Large Transactions
Code:        LARGE_TXN_LIST
Source:      SQL Query returning multiple rows
Source SQL:
  SELECT t.reference, p.name, t.amount, t.date_created
  FROM res_customer_transaction t
  JOIN res_partner p ON p.id = t.customer_id
  WHERE t.amount >= 5000000
  ORDER BY t.date_created DESC
```

---

### Step 3 — Upload a Report Template

> **Menu:** Compliance Configuration → Regulatory → Report Templates

This is where you upload the blank Excel file from the regulator.

**Requirements:**
- The file **must be `.xlsx`** (Excel 2007+). If you have an old `.xls` file, open it in
  Excel/LibreOffice and do **File → Save As → Excel Workbook (.xlsx)**.
- Do **not** upload `.xls`, `.csv`, `.pdf` — the engine cannot process those.

Fields to fill:

| Field | What to put |
|---|---|
| **Name** | Descriptive name, e.g. `CBN Monthly Prudential Return` |
| **Code** | Short code, e.g. `CBN-PRUD-MONTHLY` |
| **Reporting Entity** | Pick the regulator (CBN, FIRS, etc.) |
| **Report Type** | Select `Excel (.xlsx)` |
| **Template File** | Upload the `.xlsx` file from the regulator |

---

### Step 4 — Add Template Items (cell mappings)

Still on the Report Template form, go to the **Report Items** tab (the one2many at the
bottom). Each row maps one cell in the Excel to one Report Item.

Click **Add a line** for each cell you want to fill:

| Field | What to put |
|---|---|
| **Name** | Cell reference in the Excel file, e.g. `B12` or `C5` |
| **Worksheet** | The sheet tab name in the Excel file, e.g. `Summary` |
| **Report Item** | The Report Item whose value goes into this cell |

**How to find the right cell reference:**
1. Open the blank regulator template in Excel
2. Click the cell that should contain the value (e.g. "Total Deposits")
3. The cell reference appears in the Name Box (top-left, usually shows `B12`)
4. Note the sheet tab name at the bottom of the screen (e.g. `Summary`)
5. Use those exact values in the Template Item

**Example mapping table for a CBN return:**

| Cell (Name) | Worksheet | Report Item |
|---|---|---|
| `B5` | Summary | Report Period Label |
| `D12` | Summary | Total Deposits |
| `D13` | Summary | Total Withdrawals |
| `A20` | Transactions | Large Transactions |

> **Note:** For SQL Multi items that return a table of rows, put the cell reference of the
> *first row* where data should start. The engine will insert rows downward from there,
> copying the cell style of that row.

---

### Step 5 — Run a Report

> **Menu:** Compliance Root → Regulatory → Reports

Create a new Report record:

| Field | What to put |
|---|---|
| **Name** | e.g. `CBN March 2026 Monthly Return` |
| **Report Template** | Pick the template you set up in Step 3 |
| **Period Start** | First day of the reporting period |
| **Period End** | Last day of the reporting period |
| **Run Mode** | `Manual` (for on-demand) or `Automated` |

Click the **Run Report** button in the form header.

The engine will:
1. Load the blank Excel template
2. For each Template Item, call the Report Item's data source (SQL or static)
3. Write the result into the correct cell on the correct sheet
4. Save the filled-in file as a new **Report Run** record

Scroll down to the **Report Runs** tab to see the output. Click the run record and
download the **Report File**.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Please upload a template file first` | No file on the template | Go to Report Templates and upload the `.xlsx` file |
| `File is not a zip file` | Template is `.xls` not `.xlsx` | Open in Excel → Save As → Excel Workbook (.xlsx) and re-upload |
| `Could not open the template as an Excel file` | File is corrupted or wrong format | Re-export from Excel as `.xlsx` and re-upload |
| `KeyError: 'Summary'` | Worksheet name in Template Item doesn't match the actual sheet tab name | Open the Excel and copy the exact tab name |
| SQL item returns nothing | SQL returns zero rows | Check your SQL in Report Items → Validate SQL Query |

---

## How the Engine Fills Cells

When you click **Run Report**, for each Template Item the engine does:

```
Report Item source = 'static'     →  writes Source Value directly into the cell
Report Item source = 'sql_single' →  runs SQL, takes the first column of the first row
Report Item source = 'sql_multi'  →  runs SQL, inserts one Excel row per result row
                                      starting at the cell reference, copies row styles
```

For `sql_multi`, the query columns map left-to-right to Excel columns starting from column A
of the target row. Make sure your SELECT column order matches the Excel column order.

---

## Model Reference

| Model | Menu path | Purpose |
|---|---|---|
| `res.regulatory.report.entity` | Config → Regulatory → Report Entities | Regulators (CBN, FIRS, …) |
| `res.regulatory.report.item` | Config → Regulatory → Report Items | Data sources (SQL / static) |
| `res.regulatory.report.template` | Config → Regulatory → Report Templates | Blank Excel templates + cell mappings |
| `res.regulatory.report.template.item` | Config → Regulatory → Report Template Items | Individual cell-to-item mappings |
| `res.regulatory.report` | Regulatory → Reports | Report runs |
| `res.regulatory.report.run` | Inside a Report record → Report Runs tab | Output files per run |

---

## Notes on SQL Queries

- Queries run as the Odoo DB user — they have read access to all tables.
- Do not use `date_from` / `date_to` variables in SQL; the engine does not inject them
  automatically. Either hardcode the period or use `date_trunc` / `now()` expressions.
- If you need the report period in the SQL, create a `static` Report Item containing the
  date string and reference it separately.
- Always test with **Validate SQL Query** before linking to a template.
