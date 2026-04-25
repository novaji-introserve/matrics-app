from odoo import api, fields, models


class SanctionList(models.Model):
    _name = "sanction.list"
    _description = "Sanction List"
    _rec_name = "name"
    _order = "name, id"

    _sql_constraints = [
        (
            "sanction_list_detail_url_unique",
            "unique(detail_url)",
            "Each sanction list record must have a unique detail URL.",
        ),
    ]

    list_type = fields.Selection(
        [("individual", "Individual"), ("entity", "Entity"),('vessel','Vessel'),
        ('airplane','Airplane'),('crypto','CryptoWallet'),('legalentity','Legal Entity'),
        ('security','Security'),('other','Other')],
        string="List Type",
        index=True,
    )
    list_sn = fields.Char(string="List S/N", index=True)

    # Compatibility field used by the existing screening query.
    name = fields.Char(
        string="Name",
        compute="_compute_name",
        inverse="_inverse_name",
        store=True,
        index=True,
    )
    first_name = fields.Char(string="First Name", tracking=True, index=True)
    surname = fields.Char(string="Surname", tracking=True, index=True)
    middle_name = fields.Char(string="Middle Name")
    entity_name = fields.Char(string="Entity Name", tracking=True, index=True)

    nationality = fields.Char(string="Nationality")
    birth_country = fields.Char(string="Birth Country")
    gender = fields.Char(string="Gender")
    title = fields.Char(string="Title")
    designation = fields.Char(string="Designation")
    date_of_birth = fields.Char(string="Date Of Birth")
    place_of_birth = fields.Char(string="Place Of Birth")
    national_id_number = fields.Char(string="National ID Number")
    passport_details = fields.Text(string="Passport Details")

    reference_number = fields.Char(string="Reference Number", index=True)
    sanction_id = fields.Char(
        string="Sanction ID",
        related="reference_number",
        store=True,
        readonly=True,
    )
    incorporation_number = fields.Char(string="Incorporation Number")
    incorporation_date = fields.Char(string="Incorporation Date")
    sanction_date = fields.Char(string="Sanction Date")
    record_date_list = fields.Char(string="Record Date List")
    record_date_detail = fields.Char(string="Record Date Detail")

    address = fields.Text(string="Address")
    aliases = fields.Text(string="Aliases")
    phone_numbers = fields.Text(string="Phone Numbers")
    comments = fields.Text(string="Comments")
    reason_for_designation = fields.Text(string="Reason For Designation")
    press_release = fields.Text(string="Press Release")
    narrative_summary = fields.Text(string="Narrative Summary")
    detail_url = fields.Char(string="Detail URL", required=False, index=True)

    source = fields.Char(string="Source", tracking=True, index=True, default="NIGSAC")
    active = fields.Boolean(
        default=True,
        help="Set to false to hide the record without deleting it.",
    )

    @api.depends("list_type", "entity_name", "first_name", "middle_name", "surname")
    def _compute_name(self):
        for record in self:
            if record.name:
                continue

            if record.list_type == "entity":
                record.name = (record.entity_name or "").strip()
                continue

            parts = [
                (record.first_name or "").strip(),
                (record.middle_name or "").strip(),
                (record.surname or "").strip(),
            ]
            record.name = " ".join(part for part in parts if part)

    def _inverse_name(self):
        # Allow imported or manually-entered name values to be stored directly.
        return

    def init(self):
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS sanction_list_name_idx ON sanction_list (name)"
        )
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS sanction_list_first_name_idx ON sanction_list (first_name)"
        )
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS sanction_list_surname_idx ON sanction_list (surname)"
        )
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS sanction_list_entity_name_idx ON sanction_list (entity_name)"
        )
