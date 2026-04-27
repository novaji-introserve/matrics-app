#!/usr/bin/env python3
"""
Compliance Management Demo Data Generator
==========================================
Run with:
    python odoo-bin shell -c odoo.conf -d <your_db> --no-http < generate_demo_data.py

Or inside an Odoo shell session, paste the content.

CONFIGURATION:
    Modify the DEMO_CONFIG dictionary below to control record counts.
    Set count to 0 to skip a table entirely.
"""

import random
import string
from datetime import date, datetime, timedelta

env = env  # noqa: F821 — provided by odoo-bin shell

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION - Modify these values to control data generation
# ═══════════════════════════════════════════════════════════════

DEMO_CONFIG = {
    # Reference Data (Small counts recommended)
    'regions': {'count': 6, 'enabled': True},           # 6 Nigerian geopolitical zones
    'branches': {'count': 5, 'enabled': True},          # Branch locations
    'genders': {'count': 2, 'enabled': True},           # Male/Female
    'tiers': {'count': 3, 'enabled': True},             # Tier 1/2/3
    'sectors': {'count': 12, 'enabled': True},          # Industry sectors
    'industries': {'count': 12, 'enabled': True},       # Customer industry classifications
    'education_levels': {'count': 7, 'enabled': True},  # Education levels
    'id_types': {'count': 6, 'enabled': True},          # ID document types
    'kyc_limits': {'count': 3, 'enabled': True},        # KYC limit configurations
    'customer_statuses': {'count': 7, 'enabled': True}, # Status configurations
    'type_configs': {'count': 7, 'enabled': True},      # Customer type mappings
    'account_officers': {'count': 30, 'enabled': True},  # Relationship managers
    'transaction_types': {'count': 10, 'enabled': True}, # Transaction type codes
    'account_types': {'count': 6, 'enabled': True},     # Account product types

    # Master Data (Scalable)
    'individual_customers': {'count': 20, 'enabled': True},   # Individual customers
    'corporate_customers': {'count': 20, 'enabled': True},    # Corporate entities
    'accounts_per_customer': {'min': 1, 'max': 3},            # Accounts per customer

    # Compliance Lists (Scalable)
    'pep_entries': {'count': 20, 'enabled': True},            # Politically Exposed Persons
    'watchlist_entries': {'count': 15, 'enabled': True},      # Watchlist entries
    'blacklist_entries': {'count': 10, 'enabled': True},      # Blacklisted entities
    'sanction_entries': {'count': 10, 'enabled': True},       # Sanction list entries

    # Screening & Transactions
    'screening_per_high_risk': {'min': 1, 'max': 2},          # Screenings per high-risk customer
    'transactions_per_account': {'min': 3, 'max': 10},        # Transactions per account
    'max_accounts_for_transactions': 100,                     # Limit accounts to generate transactions for
}

# Data pools for generating realistic Nigerian names and data
FIRST_NAMES_MALE = [
    'Abdul', 'Abubakar', 'Ade', 'Adebayo', 'Adebola', 'Adegoke', 'Ademola', 'Adewale', 'Ahmed', 'Akin',
    'Akintunde', 'Aliyu', 'Aminu', 'Babatunde', 'Bashir', 'Bola', 'Chidi', 'Chidubem', 'Chijioke', 'Chinedu',
    'Chukwuemeka', 'Chukwuma', 'Daniel', 'David', 'Emeka', 'Emmanuel', 'Femi', 'Gafar', 'Garba', 'Gbolahan',
    'Hassan', 'Ibrahim', 'Ifeanyi', 'Ikechukwu', 'Isaac', 'Jide', 'John', 'Joseph', 'Jude', 'Kabiru',
    'Kazeem', 'Kelechi', 'Kunle', 'Micheal', 'Moses', 'Musa', 'Mustapha', 'Nnamdi', 'Obi', 'Obinna',
    'Okechukwu', 'Olumide', 'Oluwaseun', 'Onyeka', 'Paul', 'Peter', 'Rasheed', 'Rotimi', 'Saheed', 'Samuel',
    'Sanusi', 'Segun', 'Suleiman', 'Sunday', 'Taiwo', 'Temitope', 'Tunde', 'Uche', 'Umar', 'Usman',
    'Victor', 'Wale', 'Yakubu', 'Yusuf'
]

FIRST_NAMES_FEMALE = [
    'Abigail', 'Adaeze', 'Adebimpe', 'Adebola', 'Adenike', 'Aderonke', 'Adesua', 'Adewunmi', 'Aisha', 'Amaka',
    'Amina', 'Augustina', 'Bisola', 'Blessing', 'Chiamaka', 'Chidinma', 'Chinelo', 'Chinwe', 'Chioma', 'Christiana',
    'Comfort', 'Damilola', 'Esther', 'Fadekemi', 'Fati', 'Fatima', 'Folasade', 'Funke', 'Funmilayo', 'Grace',
    'Halima', 'Hannah', 'Ifeoma', 'Ifunanya', 'Ijeoma', 'Joy', 'Kemi', 'Latifat', 'Mariam', 'Mary',
    'Mercy', 'Moji', 'Ngozi', 'Nkechi', 'Nkiru', 'Nwakaego', 'Obiageli', 'Oluwakemi', 'Omobolanle', 'Omolara',
    'Omotola', 'Onyeka', 'Patience', 'Patricia', 'Rashida', 'Rose', 'Sade', 'Sandra', 'Sarah', 'Sikiru',
    'Stella', 'Titilayo', 'Tolani', 'Tolulope', 'Uchechi', 'Uzoamaka', 'Victoria', 'Yetunde', 'Yewande', 'Zainab'
]

LAST_NAMES = [
    'Abdullahi', 'Adebayo', 'Adegbite', 'Adekunle', 'Adelakun', 'Ademola', 'Adeniyi', 'Adeyemi', 'Adeyeye', 'Adewale',
    'Adeyinka', 'Adigun', 'Afolabi', 'Ajayi', 'Akindele', 'Akintola', 'Akinyemi', 'Aliyu', 'Amadi', 'Aminu',
    'Anozie', 'Arowolo', 'Awolowo', 'Ayodele', 'Azikiwe', 'Babatunde', 'Badmus', 'Balogun', 'Bello', 'Chukwu',
    'Chukwuma', 'Danjuma', 'Eze', 'Ezeani', 'Fashola', 'Fasola', 'Gbadamosi', 'Hassan', 'Ibrahim', 'Idris',
    'Igwe', 'Ikokwu', 'Kalu', 'Kanu', 'Lawal', 'Madu', 'Mohammed', 'Musa', 'Nnamani', 'Nwachukwu',
    'Nwosu', 'Obi', 'Obiagu', 'Okafor', 'Okonkwo', 'Okorie', 'Olatunji', 'Olawale', 'Olowe', 'Oluwole',
    'Onuoha', 'Okafor', 'Okeke', 'Okonkwo', 'Okorie', 'Olatunji', 'Olawale', 'Olowe', 'Oluwole', 'Onuoha',
    'Oparaku', 'Osei', 'Oshodi', 'Osuji', 'Oyebanji', 'Sani', 'Suleiman', 'Tijani', 'Uche', 'Udo',
    'Umar', 'Usman', 'Yakubu', 'Yusuf'
]

MIDDLE_NAMES = [
    'Ade', 'Aliyu', 'Amina', 'Babatunde', 'Blessing', 'Chidi', 'Chinedu', 'Christian', 'David', 'Emmanuel',
    'Grace', 'Hassan', 'Ibrahim', 'James', 'John', 'Joseph', 'Kemi', 'Mary', 'Mohammed', 'Ngozi',
    'Oluwaseun', 'Paul', 'Peter', 'Sani', 'Suleiman', 'Uche', 'Yusuf'
]

CORPORATE_PREFIXES = [
    'Nigerian', 'West African', 'Lagos', 'Abuja', 'Kano', 'Port Harcourt', 'Enugu', 'Ibadan',
    'United', 'Royal', 'First', 'Global', 'National', 'International', 'Continental', 'Supreme',
    'Prime', 'Standard', 'Diamond', 'Gold', 'Silver', 'Atlantic', 'Pacific', 'Central'
]

CORPORATE_SUFFIXES = [
    'Ltd', 'Limited', 'Plc', 'Services Ltd', 'Solutions Ltd', 'Nigeria Ltd', 'Group Ltd', 
    'Industries Ltd', 'Enterprises Ltd', 'Ventures Ltd', 'Holdings Ltd', 'Consulting Ltd',
    'Technologies Ltd', 'Logistics Ltd', 'Investment Ltd', 'Properties Ltd'
]

CORPORATE_NAMES = [
    'Dangote', 'Flour Mills', 'Nigerian Breweries', 'Guinness', 'Nestle', 'Unilever', 'PZ Cussons',
    'Cadbury', 'Coca-Cola', 'Pepsi', 'MTN', 'Airtel', 'Glo', '9mobile', 'NNPC', 'Total',
    'Shell', 'Chevron', 'ExxonMobil', 'Oando', 'Conoil', 'Forte Oil', 'Zenith Bank', 'GTBank',
    'Access Bank', 'UBA', 'First Bank', 'Union Bank', 'Fidelity Bank', 'Stanbic IBTC', 'Ecobank',
    'Wema Bank', 'Polaris Bank', 'Unity Bank', 'Keystone Bank', 'Heritage Bank', 'Jaiz Bank',
    'Lafarge', 'Dangote Cement', 'BUA Cement', 'Ashaka Cement', 'Julius Berger', 'Setraco',
    'Dantata & Sawoe', "Cappa & D'Alberto", 'ITB Nigeria', 'Arup', 'Globacom', 'MainOne',
    'IHS Towers', 'Airtel Africa', 'Ikeja Electric', 'Eko Disco', 'Abuja Disco', 'Kano Disco',
    'Transcorp', 'UBN Property', 'UPDC', 'Julius Berger', 'Lafarge Africa', 'BUA Foods',
    'Honeywell Flour', 'FMN', 'Chi Limited', 'Promasidor', 'De-United Foods', 'Sunny Foods'
]

SECTOR_NAMES = [
    'Agriculture', 'Banking & Finance', 'Construction', 'Education', 'Healthcare',
    'Information Technology', 'Manufacturing', 'Oil & Gas', 'Real Estate',
    'Retail & Trading', 'Telecommunications', 'Transport & Logistics'
]

# (name, code) pairs — code must be unique and non-null
INDUSTRY_NAMES = [
    ('Agribusiness',               'AGRI'),
    ('Financial Services',         'FINS'),
    ('Civil Engineering',          'CENG'),
    ('Education & Training',       'EDTR'),
    ('Pharmaceuticals & Health',   'PHRM'),
    ('Software & IT Services',     'SITS'),
    ('Heavy Manufacturing',        'HMFG'),
    ('Petroleum & Energy',         'PENG'),
    ('Property & Mortgage',        'PROP'),
    ('Consumer Goods & Retail',    'CGRT'),
    ('Telecom & Media',            'TMED'),
    ('Shipping & Logistics',       'SLOG'),
]

TIER_NAMES = [
    ('Tier 1 - Basic', 'T1', '1'),
    ('Tier 2 - Standard', 'T2', '2'),
    ('Tier 3 - Premium', 'T3', '3')
]

EDUCATION_NAMES = [
    ('No Formal Education', 'NFE'),
    ('Primary School', 'PRI'),
    ('Secondary School', 'SEC'),
    ('OND / NCE', 'OND'),
    ('HND / BSc', 'HND'),
    ('MSc / MBA', 'MSC'),
    ('PhD', 'PHD')
]

ID_TYPE_NAMES = [
    ('National ID Card', 'NIN'),
    ('International Passport', 'INT'),
    ("Voter's Card", 'VTR'),
    ("Driver's Licence", 'DRV'),
    ('CAC Certificate', 'CAC'),
    ('TIN Certificate', 'TIN')
]

KYC_LIMIT_NAMES = [
    ('Tier 1 - N300,000 limit', 'KYC1'),
    ('Tier 2 - N5,000,000 limit', 'KYC2'),
    ('Tier 3 - Unlimited', 'KYC3')
]

CUST_STATUS_NAMES = [
    ('ACTIVE', 'Active Customer', 'ACT', 'Individual'),
    ('INACTIVE', 'Inactive Customer', 'INA', 'Individual'),
    ('DORMANT', 'Dormant Account', 'DOR', 'Individual'),
    ('CORPORATE', 'Corporate Entity', 'COR', 'Corporate'),
    ('GOVT', 'Government Entity', 'GOV', 'Corporate'),
    ('SME', 'Small & Medium Enterprise', 'SME', 'Corporate'),
    ('INDIVIDUAL', 'Individual Customer', 'IND', 'Individual')
]

ACCT_TYPE_NAMES = [
    ('Savings Account', 'SAV'),
    ('Current Account', 'CUR'),
    ('Fixed Deposit', 'FXD'),
    ('Domiciliary Account', 'DOM'),
    ('Corporate Current', 'CCA'),
    ('Salary Account', 'SAL')
]

TRAN_TYPE_DATA = [
    ('TRF', 'Transfer', 'debit', 'TRF'),
    ('DEP', 'Deposit', 'credit', 'DEP'),
    ('WDR', 'Withdrawal', 'debit', 'WDR'),
    ('POS', 'POS Payment', 'debit', 'POS'),
    ('ATM', 'ATM Withdrawal', 'debit', 'ATM'),
    ('INT', 'Interest Credit', 'credit', 'INT'),
    ('CHQ', 'Cheque Payment', 'debit', 'CHQ'),
    ('NIP', 'NIP Transfer', 'debit', 'NIP'),
    ('RTN', 'Return Credit', 'credit', 'RTN'),
    ('FEE', 'Bank Charges', 'debit', 'FEE')
]

PEP_POSITIONS = [
    'Senator', 'Governor', 'Minister', 'Director General', 'Permanent Secretary',
    'House of Reps Member', 'Commissioner', 'Special Adviser', 'State Governor',
    'Federal Minister', 'Local Government Chairman', 'Secretary to Government',
    'Chief of Staff', 'Head of Service', 'Ambassador', 'Chief Justice', 'Justice'
]

WATCHLIST_SOURCES = ['EFCC', 'NFIU', 'CBN', 'INTERPOL', 'NDLEA', 'ICPC', 'Police']
SANCTION_SOURCES = ['OFAC', 'UN', 'EU', 'FATF', 'UK', 'AU']

NIGERIAN_STATES = [
    'Lagos', 'Abuja', 'Rivers', 'Kano', 'Enugu', 'Delta', 'Oyo', 'Kaduna', 'Ogun', 'Anambra',
    'Imo', 'Abia', 'Akwa Ibom', 'Bauchi', 'Benue', 'Borno', 'Cross River', 'Ebonyi', 'Edo', 'Ekiti',
    'Gombe', 'Jigawa', 'Kebbi', 'Kogi', 'Kwara', 'Nasarawa', 'Niger', 'Ondo', 'Osun', 'Plateau',
    'Sokoto', 'Taraba', 'Yobe', 'Zamfara', 'Bayelsa'
]

TOWNS_BY_STATE = {
    'Lagos': ['Ikeja', 'Victoria Island', 'Lekki', 'Yaba', 'Surulere', 'Ikorodu', 'Epe'],
    'Abuja': ['Maitama', 'Wuse', 'Garki', 'Asokoro', 'Jabi', 'Kubwa', 'Gwarinpa'],
    'Rivers': ['Port Harcourt', 'Obio-Akpor', 'Eleme', 'Oyigbo', 'Ikwerre'],
    'Kano': ['Sabon Gari', 'Fagge', 'Nasarawa GRA', 'Bompai', 'Sharada', 'Sabon Gari'],
    'Enugu': ['Independence Layout', 'New Haven', 'GRA', 'Trans Ekulu', 'Achara Layout'],
    'Delta': ['Warri', 'Asaba', 'Sapele', 'Ughelli', 'Agbor'],
    'Oyo': ['Ibadan', 'Ogbomosho', 'Iseyin', 'Saki', 'Eruwa'],
    'Kaduna': ['Kaduna North', 'Kaduna South', 'Barnawa', 'Ungwan Rimi', 'Malali']
}

print("=" * 60)
print("  iComply Compliance Demo Data Generator")
print("=" * 60)
print(f"  Configuration loaded. Generating demo data...")
print("=" * 60)

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────

def rand_bvn():
    return ''.join(random.choices(string.digits, k=11))

def rand_date(start_year=1960, end_year=2000):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_phone():
    networks = ['803', '805', '807', '811', '815', '816', '905', '906', '907']
    return '0' + random.choice(networks) + ''.join(random.choices(string.digits, k=7))

def rand_amount(lo=1_000, hi=50_000_000):
    return round(random.uniform(lo, hi), 2)

def exists(model, domain):
    return env[model].search_count(domain) > 0

def get_or_create(model, vals, search_field='name'):
    rec = env[model].search([(search_field, '=', vals[search_field])], limit=1)
    if rec:
        return rec
    return env[model].create(vals)

def generate_name(gender=None):
    """Generate a realistic Nigerian name"""
    if gender is None:
        gender = random.choice(['M', 'F'])

    if gender == 'M':
        first = random.choice(FIRST_NAMES_MALE)
    else:
        first = random.choice(FIRST_NAMES_FEMALE)

    last = random.choice(LAST_NAMES)
    middle = random.choice(MIDDLE_NAMES)
    return first, middle, last

def generate_corporate_name():
    """Generate a realistic corporate name"""
    if random.random() < 0.3 and CORPORATE_NAMES:
        base = random.choice(CORPORATE_NAMES)
    else:
        prefix = random.choice(CORPORATE_PREFIXES)
        industry = random.choice([
            'Trading', 'Services', 'Consulting', 'Technologies', 'Solutions',
            'Enterprises', 'Ventures', 'Holdings', 'Investments', 'Properties',
            'Logistics', 'Construction', 'Engineering', 'Agriculture', 'Foods'
        ])
        base = f"{prefix} {industry}"

    suffix = random.choice(CORPORATE_SUFFIXES)
    return f"{base} {suffix}"

def generate_email(first, last, domain=None):
    """Generate email from names"""
    if domain is None:
        domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'mail.ng']
        domain = random.choice(domains)

    formats = [
        f"{first.lower()}.{last.lower()}@{domain}",
        f"{first.lower()}{last.lower()}@{domain}",
        f"{first[0].lower()}{last.lower()}@{domain}",
        f"{first.lower()}{last[0].lower()}@{domain}",
        f"{first.lower()}_{last.lower()}@{domain}"
    ]
    return random.choice(formats)

def get_risk_level(tier_idx=None):
    """Determine risk level with tier influence"""
    if tier_idx == 2:  # Tier 3 - higher chance of high risk
        weights = [0.2, 0.3, 0.5]
    elif tier_idx == 0:  # Tier 1 - mostly low risk
        weights = [0.7, 0.2, 0.1]
    else:
        weights = [0.4, 0.4, 0.2]

    return random.choices(['low', 'medium', 'high'], weights=weights)[0]

# Statistics tracking
generation_stats = {}

def track_stat(key, count):
    generation_stats[key] = count
    return count

# ──────────────────────────────────────────────────────────────
# 1. REGIONS
# ──────────────────────────────────────────────────────────────
regions = []
if DEMO_CONFIG['regions']['enabled'] and DEMO_CONFIG['regions']['count'] > 0:
    print("\n[1/22] Creating Regions...")
    regions_data = [
        {'name': 'North Central', 'code': 'NC', 'risk_rating': 'medium'},
        {'name': 'North East',    'code': 'NE', 'risk_rating': 'high'},
        {'name': 'North West',    'code': 'NW', 'risk_rating': 'high'},
        {'name': 'South East',    'code': 'SE', 'risk_rating': 'low'},
        {'name': 'South South',   'code': 'SS', 'risk_rating': 'medium'},
        {'name': 'South West',    'code': 'SW', 'risk_rating': 'low'},
    ]
    target = min(DEMO_CONFIG['regions']['count'], len(regions_data))
    for r in regions_data[:target]:
        rec = env['res.partner.region'].search([('code', '=', r['code'])], limit=1)
        if not rec:
            rec = env['res.partner.region'].create(r)
        regions.append(rec)
    track_stat('Regions', len(regions))
    print(f"   {len(regions)} regions ready.")

# ──────────────────────────────────────────────────────────────
# 2. BRANCHES
# ──────────────────────────────────────────────────────────────
branches = []
if DEMO_CONFIG['branches']['enabled'] and DEMO_CONFIG['branches']['count'] > 0:
    print("\n[2/22] Creating Branches...")
    base_branches = [
        {'name': 'Lagos Island Branch',   'code': 'LGB001', 'zone': 'Lagos', 'town_area': 'Lagos Island'},
        {'name': 'Abuja Main Branch',     'code': 'ABJ001', 'zone': 'FCT',   'town_area': 'Maitama'},
        {'name': 'Port Harcourt Branch',  'code': 'PHC001', 'zone': 'Rivers', 'town_area': 'GRA'},
        {'name': 'Kano Central Branch',   'code': 'KAN001', 'zone': 'Kano',  'town_area': 'Sabon Gari'},
        {'name': 'Enugu Branch',          'code': 'ENU001', 'zone': 'Enugu', 'town_area': 'Independence Layout'},
        {'name': 'Ibadan Branch',         'code': 'IBA001', 'zone': 'Oyo',   'town_area': 'Ring Road'},
        {'name': 'Warri Branch',          'code': 'WAR001', 'zone': 'Delta', 'town_area': 'Effurun'},
        {'name': 'Kaduna Branch',         'code': 'KAD001', 'zone': 'Kaduna', 'town_area': 'Barnawa'},
    ]

    target = min(DEMO_CONFIG['branches']['count'], len(base_branches))
    for i, b in enumerate(base_branches[:target]):
        if regions:
            b['region_id'] = regions[i % len(regions)].id
        rec = env['res.branch'].search([('code', '=', b['code'])], limit=1)
        if not rec:
            rec = env['res.branch'].create(b)
        branches.append(rec)

    # Generate additional branches if needed
    while len(branches) < DEMO_CONFIG['branches']['count']:
        idx = len(branches)
        state = random.choice(NIGERIAN_STATES)
        town = random.choice(TOWNS_BY_STATE.get(state, [f"{state} Central"]))
        b = {
            'name': f"{state} Branch {idx}",
            'code': f"BR{idx:03d}",
            'zone': state,
            'town_area': town
        }
        if regions:
            b['region_id'] = random.choice(regions).id
        rec = env['res.branch'].search([('code', '=', b['code'])], limit=1)
        if not rec:
            rec = env['res.branch'].create(b)
        branches.append(rec)

    track_stat('Branches', len(branches))
    print(f"   {len(branches)} branches ready.")

# ──────────────────────────────────────────────────────────────
# 3. GENDERS
# ──────────────────────────────────────────────────────────────
genders = []
if DEMO_CONFIG['genders']['enabled'] and DEMO_CONFIG['genders']['count'] > 0:
    print("\n[3/22] Creating Genders...")
    gender_names = ['Male', 'Female', 'Other', 'Prefer not to say']
    target = min(DEMO_CONFIG['genders']['count'], len(gender_names))
    for name in gender_names[:target]:
        rec = get_or_create('res.partner.gender', {'name': name})
        genders.append(rec)
    track_stat('Genders', len(genders))
    print(f"   {len(genders)} genders ready.")

# ──────────────────────────────────────────────────────────────
# 4. CUSTOMER TIERS
# ──────────────────────────────────────────────────────────────
tiers = []
if DEMO_CONFIG['tiers']['enabled'] and DEMO_CONFIG['tiers']['count'] > 0:
    print("\n[4/22] Creating Customer Tiers...")
    target = min(DEMO_CONFIG['tiers']['count'], len(TIER_NAMES))
    for name, code, level in TIER_NAMES[:target]:
        vals = {'name': name, 'code': code, 'tier_level': level, 'status': 'active'}
        rec = env['res.partner.tier'].search([('code', '=', code)], limit=1)
        if not rec:
            rec = env['res.partner.tier'].create(vals)
        tiers.append(rec)
    track_stat('Tiers', len(tiers))
    print(f"   {len(tiers)} tiers ready.")

# ──────────────────────────────────────────────────────────────
# 5. SECTORS
# ──────────────────────────────────────────────────────────────
sectors = []
if DEMO_CONFIG['sectors']['enabled'] and DEMO_CONFIG['sectors']['count'] > 0:
    print("\n[5/22] Creating Sectors...")
    target = min(DEMO_CONFIG['sectors']['count'], len(SECTOR_NAMES))
    for i, name in enumerate(SECTOR_NAMES[:target]):
        code = name[:3].upper().replace(' ', '').replace('&', '')
        vals = {'name': name, 'code': code, 'status': 'active'}
        rec = env['res.partner.sector'].search([('code', '=', code)], limit=1)
        if not rec:
            rec = env['res.partner.sector'].create(vals)
        sectors.append(rec)
    track_stat('Sectors', len(sectors))
    print(f"   {len(sectors)} sectors ready.")

# ──────────────────────────────────────────────────────────────
# 5b. CUSTOMER INDUSTRIES  (customer.industry — separate from sector)
# ──────────────────────────────────────────────────────────────
industries = []
if DEMO_CONFIG.get('industries', {}).get('enabled') and DEMO_CONFIG['industries']['count'] > 0:
    print("\n[5b/22] Creating Customer Industries...")
    target = min(DEMO_CONFIG['industries']['count'], len(INDUSTRY_NAMES))
    for name, code in INDUSTRY_NAMES[:target]:
        rec = env['customer.industry'].search([('code', '=', code)], limit=1)
        if not rec:
            rec = env['customer.industry'].create({'name': name, 'code': code})
        industries.append(rec)
    track_stat('Industries', len(industries))
    print(f"   {len(industries)} industries ready.")

# ──────────────────────────────────────────────────────────────
# 6. EDUCATION LEVELS
# ──────────────────────────────────────────────────────────────
edu_levels = []
if DEMO_CONFIG['education_levels']['enabled'] and DEMO_CONFIG['education_levels']['count'] > 0:
    print("\n[6/22] Creating Education Levels...")
    target = min(DEMO_CONFIG['education_levels']['count'], len(EDUCATION_NAMES))
    for name, code in EDUCATION_NAMES[:target]:
        vals = {'name': name, 'code': code, 'status': 'active'}
        rec = env['res.education.level'].search([('code', '=', code)], limit=1)
        if not rec:
            rec = env['res.education.level'].create(vals)
        edu_levels.append(rec)
    track_stat('Education Levels', len(edu_levels))
    print(f"   {len(edu_levels)} education levels ready.")

# ──────────────────────────────────────────────────────────────
# 7. IDENTIFICATION TYPES
# ──────────────────────────────────────────────────────────────
id_types = []
if DEMO_CONFIG['id_types']['enabled'] and DEMO_CONFIG['id_types']['count'] > 0:
    print("\n[7/22] Creating Identification Types...")
    target = min(DEMO_CONFIG['id_types']['count'], len(ID_TYPE_NAMES))
    for name, code in ID_TYPE_NAMES[:target]:
        vals = {'name': name, 'code': code}
        rec = env['res.identification.type'].search([('code', '=', code)], limit=1)
        if not rec:
            rec = env['res.identification.type'].create(vals)
        id_types.append(rec)
    track_stat('ID Types', len(id_types))
    print(f"   {len(id_types)} ID types ready.")

# ──────────────────────────────────────────────────────────────
# 8. KYC LIMITS
# ──────────────────────────────────────────────────────────────
kyc_limits = []
if DEMO_CONFIG['kyc_limits']['enabled'] and DEMO_CONFIG['kyc_limits']['count'] > 0:
    print("\n[8/22] Creating KYC Limits...")
    target = min(DEMO_CONFIG['kyc_limits']['count'], len(KYC_LIMIT_NAMES))
    for name, code in KYC_LIMIT_NAMES[:target]:
        vals = {'name': name, 'code': code}
        rec = env['res.partner.kyc.limit'].search([('code', '=', code)], limit=1)
        if not rec:
            rec = env['res.partner.kyc.limit'].create(vals)
        kyc_limits.append(rec)
    track_stat('KYC Limits', len(kyc_limits))
    print(f"   {len(kyc_limits)} KYC limits ready.")

# ──────────────────────────────────────────────────────────────
# 9. CUSTOMER STATUSES
# ──────────────────────────────────────────────────────────────
cust_statuses = []
if DEMO_CONFIG['customer_statuses']['enabled'] and DEMO_CONFIG['customer_statuses']['count'] > 0:
    print("\n[9/22] Creating Customer Statuses...")
    target = min(DEMO_CONFIG['customer_statuses']['count'], len(CUST_STATUS_NAMES))
    for status, desc, slug, name in CUST_STATUS_NAMES[:target]:
        vals = {'customer_status': status, 'desc': desc, 'slug': slug, 'name': name}
        rec = env['customer.status'].search([('customer_status', '=', status)], limit=1)
        if not rec:
            rec = env['customer.status'].create(vals)
        cust_statuses.append(rec)
    track_stat('Customer Statuses', len(cust_statuses))
    print(f"   {len(cust_statuses)} customer statuses ready.")

# ──────────────────────────────────────────────────────────────
# 10. CUSTOMER TYPE CONFIG
# ──────────────────────────────────────────────────────────────
if DEMO_CONFIG['type_configs']['enabled'] and DEMO_CONFIG['type_configs']['count'] > 0:
    print("\n[10/22] Creating Customer Type Configurations...")
    type_configs = [
        {'customer_status': 'ACTIVE',     'customer_type': 'individual', 'description': 'Active individual customers'},
        {'customer_status': 'INACTIVE',   'customer_type': 'individual', 'description': 'Inactive individual customers'},
        {'customer_status': 'DORMANT',    'customer_type': 'individual', 'description': 'Dormant individual accounts'},
        {'customer_status': 'INDIVIDUAL', 'customer_type': 'individual', 'description': 'Standard individual customers'},
        {'customer_status': 'CORPORATE',  'customer_type': 'corporate',  'description': 'Corporate customers'},
        {'customer_status': 'GOVT',       'customer_type': 'corporate',  'description': 'Government & public sector'},
        {'customer_status': 'SME',        'customer_type': 'corporate',  'description': 'SME corporate customers'},
    ]
    target = min(DEMO_CONFIG['type_configs']['count'], len(type_configs))
    created = 0
    for tc in type_configs[:target]:
        rec = env['customer.type.config'].search([('customer_status', '=', tc['customer_status'])], limit=1)
        if not rec:
            env['customer.type.config'].create(tc)
            created += 1
    track_stat('Type Configs', created)
    print(f"   {created} type configs ready.")

# ──────────────────────────────────────────────────────────────
# 11. ACCOUNT OFFICERS
# ──────────────────────────────────────────────────────────────
officers = []
if DEMO_CONFIG['account_officers']['enabled'] and DEMO_CONFIG['account_officers']['count'] > 0:
    print("\n[11/22] Creating Account Officers...")

    # Generate base officers
    base_officers = [
        {'name': 'Chukwuemeka Obi',    'code': 'AO001', 'area': 'Lagos',   'email': 'c.obi@bank.ng'},
        {'name': 'Fatima Abubakar',    'code': 'AO002', 'area': 'Abuja',   'email': 'f.abubakar@bank.ng'},
        {'name': 'Tunde Adeyemi',      'code': 'AO003', 'area': 'Lagos',   'email': 't.adeyemi@bank.ng'},
        {'name': 'Ngozi Eze',          'code': 'AO004', 'area': 'Enugu',   'email': 'n.eze@bank.ng'},
        {'name': 'Musa Suleiman',      'code': 'AO005', 'area': 'Kano',    'email': 'm.suleiman@bank.ng'},
        {'name': 'Chidinma Okafor',    'code': 'AO006', 'area': 'P/Harcourt', 'email': 'c.okafor@bank.ng'},
    ]

    target = DEMO_CONFIG['account_officers']['count']
    for i in range(target):
        if i < len(base_officers):
            o = base_officers[i]
        else:
            fn, mn, ln = generate_name()
            area = random.choice(NIGERIAN_STATES) if NIGERIAN_STATES else 'Lagos'
            o = {
                'name': f"{fn} {ln}",
                'code': f"AO{str(i+1).zfill(3)}",
                'area': area,
                'email': generate_email(fn, ln, 'bank.ng')
            }

        rec = env['account.officers'].search([('code', '=', o['code'])], limit=1)
        if not rec:
            rec = env['account.officers'].create(o)
        officers.append(rec)

    track_stat('Account Officers', len(officers))
    print(f"   {len(officers)} account officers ready.")

# ──────────────────────────────────────────────────────────────
# 12. TRANSACTION TYPES
# ──────────────────────────────────────────────────────────────
tran_types = []
if DEMO_CONFIG['transaction_types']['enabled'] and DEMO_CONFIG['transaction_types']['count'] > 0:
    print("\n[12/22] Creating Transaction Types...")
    target = min(DEMO_CONFIG['transaction_types']['count'], len(TRAN_TYPE_DATA))
    for code, name, ttype, short in TRAN_TYPE_DATA[:target]:
        vals = {'trancode': code, 'tranname': name, 'trantype': ttype, 'transhortname': short}
        rec = env['res.transaction.type'].search([('trancode', '=', code)], limit=1)
        if not rec:
            rec = env['res.transaction.type'].create(vals)
        tran_types.append(rec)
    track_stat('Transaction Types', len(tran_types))
    print(f"   {len(tran_types)} transaction types ready.")

# ──────────────────────────────────────────────────────────────
# 13. ACCOUNT TYPES
# ──────────────────────────────────────────────────────────────
acct_types = []
if DEMO_CONFIG['account_types']['enabled'] and DEMO_CONFIG['account_types']['count'] > 0:
    print("\n[13/22] Creating Account Types...")
    target = min(DEMO_CONFIG['account_types']['count'], len(ACCT_TYPE_NAMES))
    for name, code in ACCT_TYPE_NAMES[:target]:
        vals = {'name': name, 'code': code, 'status': 'active'}
        rec = env['res.partner.account.type'].search([('code', '=', code)], limit=1)
        if not rec:
            rec = env['res.partner.account.type'].create(vals)
        acct_types.append(rec)
    track_stat('Account Types', len(acct_types))
    print(f"   {len(acct_types)} account types ready.")

# ──────────────────────────────────────────────────────────────
# 14. INDIVIDUAL CUSTOMERS
# ──────────────────────────────────────────────────────────────
individual_customers = []
if DEMO_CONFIG['individual_customers']['enabled'] and DEMO_CONFIG['individual_customers']['count'] > 0:
    print("\n[14/22] Creating Individual Customers...")

    target = DEMO_CONFIG['individual_customers']['count']
    status_active = env['customer.status'].search([('customer_status', '=', 'ACTIVE')], limit=1)

    # Predefined high-profile individuals (PEP-like)
    high_profile = [
        ('Alhaji', 'Dangote', 'Aliko', 'M'),
        ('Senator', 'Orji', 'Uzor', 'M'),
        ('Chief', 'Okafor', 'Ifeanyi', 'M'),
        ('Dr.', 'Okonjo', 'Ngozi', 'F'),
        ('Governor', 'El-Rufai', 'Nasir', 'M'),
    ]

    for idx in range(target):
        cust_id = f'IND{str(idx + 1).zfill(5)}'
        existing = env['res.partner'].search([('customer_id', '=', cust_id)], limit=1)
        if existing:
            individual_customers.append(existing)
            continue

        # Mix of generated and high-profile names
        if idx < len(high_profile) and random.random() < 0.3:
            title, ln, mn, gender = high_profile[idx % len(high_profile)]
            fn = title
        else:
            gender = random.choice(['M', 'F'])
            fn, mn, ln = generate_name(gender)

        # Determine tier and risk
        tier_idx = random.choices([0, 1, 2], weights=[0.4, 0.4, 0.2])[0] if tiers else 0
        risk = get_risk_level(tier_idx)

        # Generate DOB based on age group
        if random.random() < 0.1:  # 10% elderly
            dob = rand_date(1940, 1965)
        elif random.random() < 0.3:  # 30% young adults
            dob = rand_date(1990, 2005)
        else:  # 60% middle aged
            dob = rand_date(1965, 1990)

        vals = {
            'name': f'{fn} {mn} {ln}',
            'firstname': fn,
            'lastname': ln,
            'middlename': mn,
            'customer_id': cust_id,
            'bvn': rand_bvn(),
            'dob': str(dob),
            'phone': rand_phone(),
            'email': generate_email(fn, ln),
            'sex_id': genders[0].id if genders and gender == 'M' else (genders[1].id if len(genders) > 1 else False),
            'risk_level': risk,
            'risk_score': {'low': random.uniform(0, 30), 'medium': random.uniform(31, 60), 'high': random.uniform(61, 100)}[risk],
            'sector_id': random.choice(sectors).id if sectors else False,
            'customer_industry_id': random.choice(industries).id if industries else False,
            'tier_id': tiers[tier_idx].id if tiers and tier_idx < len(tiers) else False,
            'branch_id': random.choice(branches).id if branches else False,
            'account_officer_id': random.choice(officers).id if officers else False,
            'education_level_id': random.choice(edu_levels).id if edu_levels else False,
            'identification_type_id': random.choice(id_types[:4]).id if len(id_types) >= 4 else (id_types[0].id if id_types else False),
            'kyc_limit_id': kyc_limits[tier_idx].id if kyc_limits and tier_idx < len(kyc_limits) else False,
            'internal_category': 'customer',
            'customer_status': status_active.id if status_active else False,
            'is_company': False,
            'origin': 'demo',
        }

        # Add address info
        state = random.choice(NIGERIAN_STATES)
        vals['city'] = random.choice(TOWNS_BY_STATE.get(state, [f"{state} Town"]))

        try:
            rec = env['res.partner'].create(vals)
            individual_customers.append(rec)
        except Exception as e:
            print(f"   WARN: could not create {fn} {ln}: {e}")
            continue

        if (idx + 1) % 10 == 0:
            print(f"   ... {idx + 1}/{target} created")

    track_stat('Individual Customers', len(individual_customers))
    print(f"   {len(individual_customers)} individual customers ready.")

# ──────────────────────────────────────────────────────────────
# 15. CORPORATE CUSTOMERS
# ──────────────────────────────────────────────────────────────
corporate_customers = []
if DEMO_CONFIG['corporate_customers']['enabled'] and DEMO_CONFIG['corporate_customers']['count'] > 0:
    print("\n[15/22] Creating Corporate Customers...")

    target = DEMO_CONFIG['corporate_customers']['count']
    status_corp = env['customer.status'].search([('customer_status', '=', 'CORPORATE')], limit=1)

    # Predefined major corporates
    major_corporates = [
        'Dangote Industries Limited',
        'MTN Nigeria Communications',
        'Access Bank Plc',
        'Zenith Bank Plc',
        'Nigerian Breweries Plc',
        'Flour Mills of Nigeria',
        'Oando Plc',
        'Transcorp Hotels Plc',
        'United Bank for Africa',
        'Stallion Group Nigeria',
    ]

    for idx in range(target):
        cust_id = f'CRP{str(idx + 1).zfill(5)}'
        existing = env['res.partner'].search([('customer_id', '=', cust_id)], limit=1)
        if existing:
            corporate_customers.append(existing)
            continue

        if idx < len(major_corporates):
            name = major_corporates[idx]
        else:
            name = generate_corporate_name()

        tier_idx = random.choices([0, 1, 2], weights=[0.2, 0.3, 0.5])[0] if tiers else 0
        risk = random.choice(['low', 'medium', 'high'])

        vals = {
            'name': name,
            'customer_id': cust_id,
            'vat': f'RC{random.randint(100000, 999999)}',
            'phone': rand_phone(),
            'email': f'info@{name.lower().replace(" ", "").replace("&", "")[:15]}.ng',
            'risk_level': risk,
            'risk_score': {'low': random.uniform(0, 30), 'medium': random.uniform(31, 60), 'high': random.uniform(61, 100)}[risk],
            'sector_id': random.choice(sectors).id if sectors else False,
            'customer_industry_id': random.choice(industries).id if industries else False,
            'tier_id': tiers[tier_idx].id if tiers and tier_idx < len(tiers) else False,
            'branch_id': random.choice(branches).id if branches else False,
            'account_officer_id': random.choice(officers).id if officers else False,
            'kyc_limit_id': kyc_limits[tier_idx].id if kyc_limits and tier_idx < len(kyc_limits) else False,
            'internal_category': 'customer',
            'customer_status': status_corp.id if status_corp else False,
            'is_company': True,
            'origin': 'demo',
        }

        try:
            rec = env['res.partner'].create(vals)

            # Add directors/shareholders (2-4 per company)
            if individual_customers:
                num_directors = random.randint(2, 4)
                shareholders = []
                used_indices = set()

                for i in range(num_directors):
                    if len(used_indices) >= len(individual_customers):
                        break

                    # Pick unique individual
                    while True:
                        idx = random.randint(0, len(individual_customers) - 1)
                        if idx not in used_indices:
                            used_indices.add(idx)
                            break

                    indiv = individual_customers[idx]
                    shareholders.append((0, 0, {
                        'name': indiv.name,
                        'role': random.choice(['director', 'shareholder']),
                        'pct_equity': round(random.uniform(5, 40), 2) if random.random() < 0.7 else 0,
                        'bvn': indiv.bvn or rand_bvn(),
                    }))

                if shareholders:
                    rec.write({'shareholder_ids': shareholders})

            corporate_customers.append(rec)
        except Exception as e:
            print(f"   WARN: could not create {name}: {e}")
            continue

        if (idx + 1) % 5 == 0:
            print(f"   ... {idx + 1}/{target} created")

    track_stat('Corporate Customers', len(corporate_customers))
    print(f"   {len(corporate_customers)} corporate customers ready.")

all_customers = individual_customers + corporate_customers

# ──────────────────────────────────────────────────────────────
# 16. CUSTOMER ACCOUNTS
# ──────────────────────────────────────────────────────────────
accts_created = 0
if all_customers and DEMO_CONFIG.get('accounts_per_customer'):
    print("\n[16/22] Creating Customer Accounts...")

    account_states = ['Active', 'Inactive', 'Dormant', 'Suspended', 'Closed']

    for customer in all_customers:
        min_acct = DEMO_CONFIG['accounts_per_customer']['min']
        max_acct = DEMO_CONFIG['accounts_per_customer']['max']
        num_accounts = random.randint(min_acct, max_acct)

        for _ in range(num_accounts):
            # Generate unique account number
            while True:
                acct_no = ''.join(random.choices(string.digits, k=10))
                if not env['res.partner.account'].search_count([('name', '=', acct_no)]):
                    break

            # Determine account type based on customer type
            if customer.is_company:
                preferred_types = [at for at in acct_types if 'Corporate' in at.name or 'Current' in at.name] or acct_types
            else:
                preferred_types = acct_types

            acct_type = random.choice(preferred_types) if preferred_types else False

            # Opening date
            opening = rand_date(2010, 2023)

            # Balance based on tier
            tier_idx = 0
            if hasattr(customer, 'tier_id') and customer.tier_id:
                try:
                    tier_idx = tiers.index(customer.tier_id) if customer.tier_id in tiers else 0
                except:
                    pass

            balance_ranges = [(0, 100000), (10000, 5000000), (100000, 50000000)]
            bal_min, bal_max = balance_ranges[min(tier_idx, 2)]

            vals = {
                'name': acct_no,
                'account_name': customer.name,
                'customer_id': customer.id,
                'account_type_id': acct_type.id if acct_type else False,
                'branch_id': customer.branch_id.id if customer.branch_id else False,
                'account_officer_id': customer.account_officer_id.id if customer.account_officer_id else False,
                'balance': rand_amount(bal_min, bal_max),
                'state': random.choice(account_states),
                'opening_date': opening,
                'high_transactions_account': random.random() < 0.1,
                'closure_status': 'N',
                'currency_id': env['res.currency'].search([('name', '=', 'NGN')], limit=1).id,
            }

            try:
                env['res.partner.account'].create(vals)
                accts_created += 1
            except Exception as e:
                pass

        if accts_created % 50 == 0 and accts_created > 0:
            print(f"   ... {accts_created} accounts created")

    track_stat('Accounts Created', accts_created)
    print(f"   {accts_created} accounts created.")

# ──────────────────────────────────────────────────────────────
# 17. PEP LIST ENTRIES
# ──────────────────────────────────────────────────────────────
pep_recs = []
if DEMO_CONFIG['pep_entries']['enabled'] and DEMO_CONFIG['pep_entries']['count'] > 0:
    print("\n[17/22] Creating PEP List entries...")

    target = DEMO_CONFIG['pep_entries']['count']

    # Predefined PEPs
    base_peps = [
        ('Okeke', 'Ifeanyi', 'Chukwuma', 'Senator'),
        ('Adewale', 'Tokunbo', 'Segun', 'Governor'),
        ('Aliyu', 'Sani', 'Muhammed', 'Minister'),
        ('Okafor', 'Chinyere', 'Blessing', 'Director General'),
        ('Ibrahim', 'Garba', 'Musa', 'Permanent Secretary'),
        ('Johnson', 'Emmanuel', 'Femi', 'House of Reps Member'),
        ('Nwachukwu', 'Obiageli', 'Ada', 'Commissioner'),
        ('Salami', 'Rasheed', 'Yemi', 'Special Adviser'),
        ('Danladi', 'Abubakar', 'Usman', 'State Governor'),
        ('Badmus', 'Kafayat', 'Adeola', 'Federal Minister'),
    ]

    for idx in range(target):
        if idx < len(base_peps):
            sn, fn, mn, pos = base_peps[idx]
        else:
            gender = random.choice(['M', 'F'])
            fn, mn, sn = generate_name(gender)
            pos = random.choice(PEP_POSITIONS)

        # Check if exists
        rec = env['pep.list'].search([('firstname', '=', fn), ('lastname', '=', sn)], limit=1)
        if not rec:
            unique_id = f'PEP{random.randint(10000,99999)}'
            try:
                rec = env['pep.list'].create({
                    'firstname': fn,
                    'lastname': sn,
                    'name': f'{fn} {mn} {sn}',
                    'unique_id': unique_id,
                    'position': pos,
                })
            except Exception as e:
                print(f"   WARN PEP: {e}")
                continue
        pep_recs.append(rec)

        if (idx + 1) % 10 == 0:
            print(f"   ... {idx + 1}/{target} created")

    track_stat('PEP Entries', len(pep_recs))
    print(f"   {len(pep_recs)} PEP list entries ready.")

# ──────────────────────────────────────────────────────────────
# 18. WATCHLIST ENTRIES
# ──────────────────────────────────────────────────────────────
if DEMO_CONFIG['watchlist_entries']['enabled'] and DEMO_CONFIG['watchlist_entries']['count'] > 0:
    print("\n[18/22] Creating Watchlist entries...")

    target = DEMO_CONFIG['watchlist_entries']['count']
    created = 0

    for idx in range(target):
        gender = random.choice(['M', 'F'])
        fn, mn, sn = generate_name(gender)

        wl_id = f'{random.choice(WATCHLIST_SOURCES)}{idx+1:03d}'

        if not env['res.partner.watchlist'].search_count([('watchlist_id', '=', wl_id)]):
            try:
                env['res.partner.watchlist'].create({
                    'name': f'{fn} {mn} {sn}',
                    'surname': sn,
                    'first_name': fn,
                    'middle_name': mn,
                    'watchlist_id': wl_id,
                    'nationality': 'NGA',
                    'bvn': rand_bvn(),
                    'source': random.choice(WATCHLIST_SOURCES),
                })
                created += 1
            except Exception as e:
                print(f"   WARN Watchlist: {e}")

    track_stat('Watchlist Entries', created)
    print(f"   {created} watchlist entries ready.")

# ──────────────────────────────────────────────────────────────
# 19. BLACKLIST ENTRIES
# ──────────────────────────────────────────────────────────────
if DEMO_CONFIG['blacklist_entries']['enabled'] and DEMO_CONFIG['blacklist_entries']['count'] > 0:
    print("\n[19/22] Creating Blacklist entries...")

    target = DEMO_CONFIG['blacklist_entries']['count']
    created = 0

    for idx in range(target):
        gender = random.choice(['M', 'F'])
        fn, mn, sn = generate_name(gender)

        if not env['res.partner.blacklist'].search_count([('surname', '=', sn), ('first_name', '=', fn)]):
            try:
                env['res.partner.blacklist'].create({
                    'name': f'{fn} {mn} {sn}',
                    'surname': sn,
                    'first_name': fn,
                    'middle_name': mn,
                    'bvn': rand_bvn(),
                    'active': True,
                })
                created += 1
            except Exception as e:
                print(f"   WARN Blacklist: {e}")

    track_stat('Blacklist Entries', created)
    print(f"   {created} blacklist entries ready.")

# ──────────────────────────────────────────────────────────────
# 20. SANCTION LIST ENTRIES
# ──────────────────────────────────────────────────────────────
if DEMO_CONFIG['sanction_entries']['enabled'] and DEMO_CONFIG['sanction_entries']['count'] > 0:
    print("\n[20/22] Creating Sanction List entries...")

    target = DEMO_CONFIG['sanction_entries']['count']
    created = 0

    # Some international names for sanctions
    international_names = [
        ('Petrov', 'Vladimir', 'Alexei', 'RUS'),
        ('Kim', 'Jong', 'Un', 'PRK'),
        ('Hassan', 'Ali', 'Mahmoud', 'IRN'),
        ('Mendez', 'Carlos', 'Alberto', 'COL'),
        ('Abacha', 'Sani', 'Musa', 'NGA'),
    ]

    for idx in range(target):
        if idx < len(international_names):
            sn, fn, mn, nat = international_names[idx]
        else:
            gender = random.choice(['M', 'F'])
            fn, mn, sn = generate_name(gender)
            nat = random.choice(['NGA', 'RUS', 'IRN', 'AFG', 'PRK', 'SOM', 'SDN'])

        s_id = f'{random.choice(SANCTION_SOURCES)}-{idx+1:03d}'

        if not env['sanction.list'].search_count([('sanction_id', '=', s_id)]):
            try:
                env['sanction.list'].create({
                    'name': f'{fn} {mn} {sn}',
                    'surname': sn,
                    'first_name': fn,
                    'middle_name': mn,
                    'sanction_id': s_id,
                    'nationality': nat,
                    'source': random.choice(SANCTION_SOURCES),
                    'active': True,
                })
                created += 1
            except Exception as e:
                print(f"   WARN Sanction: {e}")

    track_stat('Sanction Entries', created)
    print(f"   {created} sanction list entries ready.")

# ──────────────────────────────────────────────────────────────
# 21. CUSTOMER SCREENING RESULTS
# ──────────────────────────────────────────────────────────────
screening_created = 0
if all_customers and DEMO_CONFIG.get('screening_per_high_risk'):
    print("\n[21/22] Creating Screening Results...")

    high_risk_customers = [c for c in all_customers if c.risk_level == 'high']

    min_screen = DEMO_CONFIG['screening_per_high_risk']['min']
    max_screen = DEMO_CONFIG['screening_per_high_risk']['max']

    for cust in high_risk_customers:
        num_screenings = random.randint(min_screen, max_screen)

        for _ in range(num_screenings):
            list_type = random.choice(['pep', 'watchlist', 'sanction'])
            state = random.choice(['pending', 'confirmed', 'dismissed'])

            # Check if already exists
            if env['res.partner.screening.result'].search_count([
                ('partner_id', '=', cust.id), ('list_type', '=', list_type)
            ]):
                continue

            try:
                env['res.partner.screening.result'].create({
                    'partner_id': cust.id,
                    'list_type': list_type,
                    'state': state,
                    'notes': f'Auto-generated screening match for {cust.name} against {list_type}',
                })

                # Update customer flags
                flag_map = {'pep': 'is_pep', 'watchlist': 'is_watchlist', 'sanction': 'likely_sanction', 'blacklist': 'is_blacklist'}
                flag_field = flag_map.get(list_type)
                if flag_field:
                    cust.write({flag_field: True})

                screening_created += 1
            except Exception as e:
                print(f"   WARN Screening: {e}")

    track_stat('Screening Results', screening_created)
    print(f"   {screening_created} screening results created.")

# ──────────────────────────────────────────────────────────────
# 22. TRANSACTIONS
# ──────────────────────────────────────────────────────────────
tran_count = 0
if DEMO_CONFIG.get('transactions_per_account'):
    print("\n[22/22] Creating Transactions...")

    all_accounts = env['res.partner.account'].search([])
    ngn_currency = env['res.currency'].search([('name', '=', 'NGN')], limit=1)

    max_accts = DEMO_CONFIG.get('max_accounts_for_transactions', 100)
    accounts_to_process = all_accounts[:max_accts]

    min_trans = DEMO_CONFIG['transactions_per_account']['min']
    max_trans = DEMO_CONFIG['transactions_per_account']['max']

    for acct in accounts_to_process:
        num_trans = random.randint(min_trans, max_trans)

        for i in range(num_trans):
            # Generate unique reference
            while True:
                ref = 'TXN' + ''.join(random.choices(string.digits, k=10))
                if not env['res.customer.transaction'].search_count([('name', '=', ref)]):
                    break

            tran_type = random.choice(tran_types) if tran_types else False

            # Amount based on account balance and transaction type
            base_amount = rand_amount(500, 5_000_000)
            if tran_type and tran_type.trantype == 'credit':
                # Credits tend to be larger
                base_amount = rand_amount(1000, 10_000_000)

            # Date within last year
            trans_date = datetime.now() - timedelta(days=random.randint(0, 365))

            vals = {
                'name': ref,
                'customer_id': acct.customer_id.id,
                'account_id': acct.id,
                'branch_id': acct.branch_id.id if acct.branch_id else False,
                'account_officer_id': acct.account_officer_id.id if acct.account_officer_id else False,
                'tran_type': tran_type.id if tran_type else False,
                'amount': base_amount,
                'amount_local': base_amount,
                'transmode_code': random.choice(['A', 'E', 'T', 'B']),
                'currency_id': ngn_currency.id if ngn_currency else False,
                'narration': f'{tran_type.tranname if tran_type else "Transaction"} - {ref}',
                'date_created': trans_date,
                'state': random.choice(['new', 'done']),
            }

            try:
                env['res.customer.transaction'].create(vals)
                tran_count += 1
            except Exception as e:
                print(f"   WARN Transaction: {e}")

        if tran_count % 100 == 0 and tran_count > 0:
            print(f"   ... {tran_count} transactions created")

    track_stat('Transactions', tran_count)
    print(f"   {tran_count} transactions created.")

# ──────────────────────────────────────────────────────────────
# 23. RISK ASSESSMENT MASTER DATA & ASSESSMENTS
# ──────────────────────────────────────────────────────────────
print("\n[23/23] Creating Risk Assessments...")

# ── Assessment Types ──────────────────────────────────────────
ra_type_data = [
    {'name': 'Customer Risk Assessment',  'code': 'CRA'},
    {'name': 'Transaction Risk Assessment', 'code': 'TRA'},
    {'name': 'Country Risk Assessment',   'code': 'CTRA'},
    {'name': 'Product Risk Assessment',   'code': 'PRA'},
]
ra_types = []
for d in ra_type_data:
    rec = env['res.risk.assessment.type'].search([('code', '=', d['code'])], limit=1)
    if not rec:
        rec = env['res.risk.assessment.type'].create(d)
    ra_types.append(rec)

# ── Risk Types ────────────────────────────────────────────────
ra_risk_type_data = [
    {'name': 'Money Laundering',          'code': 'ML'},
    {'name': 'Terrorism Financing',       'code': 'TF'},
    {'name': 'Fraud Risk',                'code': 'FR'},
    {'name': 'Reputational Risk',         'code': 'RR'},
    {'name': 'Regulatory Risk',           'code': 'REG'},
]
admin_user = env.ref('base.user_admin')
ra_risk_types = []
for d in ra_risk_type_data:
    rec = env['res.risk.type'].search([('code', '=', d['code'])], limit=1)
    if not rec:
        rec = env['res.risk.type'].create({**d, 'user_id': admin_user.id})
    ra_risk_types.append(rec)

# ── Risk Assessments per customer ─────────────────────────────
ra_created = 0
customers_for_ra = all_customers if all_customers else env['res.partner'].search([('origin', 'in', ['demo', 'test', 'prod'])], limit=50)

for cust in customers_for_ra:
    if env['res.risk.assessment'].search_count([('partner_id', '=', cust.id)]):
        continue
    risk = cust.risk_level or 'low'
    rating_map = {'low': random.uniform(1, 30), 'medium': random.uniform(31, 60), 'high': random.uniform(61, 100)}
    try:
        env['res.risk.assessment'].create({
            'name': f'Risk Assessment - {cust.name}',
            'code': f'RA-{cust.customer_id or cust.id}',
            'user_id': admin_user.id,
            'partner_id': cust.id,
            'risk_rating': round(rating_map[risk], 2),
            'assessment_type_id': random.choice(ra_types).id,
            'type_id': random.choice(ra_risk_types).id,
            'internal_category': 'cp' if cust.is_company else 'inst',
            'narration': f'<p>Auto-generated risk assessment for {cust.name}. Risk level: {risk}.</p>',
            'is_default': False,
            'active': True,
        })
        ra_created += 1
    except Exception as e:
        print(f"   WARN RiskAssessment: {e}")

track_stat('Risk Assessments', ra_created)
print(f"   {ra_created} risk assessments created.")

# ──────────────────────────────────────────────────────────────
# COMMIT
# ──────────────────────────────────────────────────────────────
env.cr.commit()

print("\n" + "=" * 60)
print("  Demo data generation COMPLETE!")
print("=" * 60)
print("""
  Configuration Used:
    - Individual Customers: {ind_count}
    - Corporate Customers: {corp_count}
    - Accounts per Customer: {acct_min}-{acct_max}
    - Transactions per Account: {trans_min}-{trans_max}

  Summary Statistics:
""".format(
    ind_count=DEMO_CONFIG['individual_customers']['count'],
    corp_count=DEMO_CONFIG['corporate_customers']['count'],
    acct_min=DEMO_CONFIG['accounts_per_customer']['min'],
    acct_max=DEMO_CONFIG['accounts_per_customer']['max'],
    trans_min=DEMO_CONFIG['transactions_per_account']['min'],
    trans_max=DEMO_CONFIG['transactions_per_account']['max']
))

for key, value in generation_stats.items():
    print(f"    {key:.<25} {value:>5}")

print("\n" + "=" * 60)
print("  All data committed to database successfully!")
print("=" * 60)