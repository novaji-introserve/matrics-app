from odoo import _, api, fields, models
import requests
import json
import markdown

class Pep(models.Model):
    _name = 'res.pep'
    _description = 'PEP List'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    _sql_constraints = [
        ('uniq_pep_identifier', 'unique(unique_identifier)',
         "Unique Identifier already exists. Value must be unique!"),
    ]
    _order="surname,first_name"
    
    narration = fields.Html(string='Narration')
    lastmodifiedon = fields.Char(string="Last Modified On")
    lastmodifiedbyemail = fields.Char(string="Last Modified by Email")
    unique_identifier = fields.Char(string="Unique Identifier", index=True,required=True)
    surname = fields.Char(string="Surname",tracking=True,required=True,index=True)
    first_name = fields.Char(string="First Name",tracking=True,required=True,index=True)
    middle_name = fields.Char(string="Middle Name")
    title = fields.Char(string="Title")
    aka = fields.Char(string="Aka")
    sex = fields.Char(string="Sex")
    date_of_birth = fields.Char(string="Date Of Birth")
    present_position = fields.Char(string="Present Position")
    previous_position = fields.Char(string="Previous Position")
    pep_classification = fields.Char(string="Pep Classification")
    official_address = fields.Char(string="Official Address")
    profession = fields.Char(string="Profession")
    residential_address = fields.Char(string="Residential Address")
    state_of_origin = fields.Char(string="State Of Origin")
    spouse = fields.Char(string="Spouse")
    children = fields.Char(string="Children")
    sibling = fields.Char(string="Sibling")
    parents = fields.Char(string="Parents")
    mothers_maden_name = fields.Char(string="Mothers Maiden Name")
    associates__business_political_social_ = fields.Char(string="Associates  Business Political Social")
    bankers = fields.Char(string="Bankers")
    account_details = fields.Char(string="Account Details")
    place_of_birth = fields.Char(string="Place Of Birth")
    press_report = fields.Text(string="Press Report")
    date_report = fields.Char(string="Date Report")
    additional_info = fields.Html(string="Additional Info")
    email = fields.Char(string="Email")
    remarks = fields.Char(string="Remarks")
    name = fields.Char(string="Name")
    status = fields.Char(string="Status")
    business_interest = fields.Char(string="Business Interest")
    age = fields.Char(string="Age")
    associate_business_politics = fields.Char(string="Associate Business Politics")
    pob = fields.Char(string="Place of Birth")
    createdby = fields.Char(string="Created By")
    createdon = fields.Char(string="Created On")
    createdbyemail = fields.Char(string="Created by Email")
    lastmodifiedby = fields.Char(string="Last Modified By")
    religion = fields.Text(string='Religion')
    citizenship = fields.Char(string='Citizenship')
    education = fields.Text(string='Education')
    career_history = fields.Text(string='Career History')
    
    @api.model
    def create(self,vals):
        if 'first_name' in vals:
            vals['name'] = self.get_name(vals)
        record = super(Pep, self).create(vals)
        return record
    
    def write(self,vals):
        if 'first_name' in vals:
            vals['name'] = self.get_name(vals)
        record = super(Pep, self).write(vals)
        return record
    
    def get_name(self,vals):
        return f"%s %s"%(vals['first_name'],vals['surname'])
    
    @api.depends('first_name','surname')
    def action_find_person(self):
        name = f"Who is %s %s"%(self.first_name,self.surname)
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Catch-Control": "no-cache"}
        json_data = {"contents":[{"parts":[{"text":f"{name}"}]}]}
        config = self.env['ir.config_parameter'].sudo()
        api_key = config.get_param('gemini_api_key')
        try:
            if api_key:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}"
                response = requests.post(url, data=json.dumps(json_data), headers=headers)
                data = json.loads(response.text)
                # Extract the 'text' tag
                text_value = data['candidates'][0]['content']['parts'][0]['text']
                self.write({'narration':markdown.markdown(text_value)})
        except:
            pass
        
        self.query_sanctions_service(self.first_name,self.surname)
        
    
    def query_sanctions_service(self,firstname,lastname):
        config = self.env['ir.config_parameter'].sudo()
        API_KEY = config.get_param('opensanctions_api_key')
        if API_KEY is not None:
            try:
                headers = {
                    "Authorization": API_KEY,
                }
                # Prepare a query to match on schema and the name property
                query = {
                    "queries": {
                        "q1": {"schema": "Person", "properties": {"name": [f"{firstname} {lastname}"]}}
                    }
                }
                # Make the request
                response = requests.post(
                    "https://api.opensanctions.org/match/default", headers=headers, json=query
                )
                # Check for HTTP errors
                response.raise_for_status()
                # Get the results for our query
                data = response.json()["responses"]["q1"]["results"]
                person = data[0]
                metadata = data[1]
                properties = person['properties']
                position = "\n".join(properties['position']) if 'position' in properties else "\n".join(metadata['properties']['position'])
                education = "\n".join(metadata['properties']['education']) if 'education' in metadata['properties'] else None
                notes = "\n".join(properties['notes']) if 'notes' in properties else None
                birth_place = "\n".join(properties['birthPlace']) if 'birthPlace' in properties else None
                religion = "\n".join(properties['religion']) if 'religion' in properties else ''
                middle_name = metadata['properties']['middleName'][0] if 'middleName' in metadata['properties'] else ''
                first_name = metadata['properties']['firstName'][0] if 'firstName' in  metadata['properties'] else " ".join(person['caption'])
                last_name = metadata['properties']['lastName'][0] if 'lastName' in metadata['properties'] else None
                title = metadata['properties']['title'][0] if 'title' in metadata['properties'] else ''
                gender = person['properties']['gender'][0].capitalize()
                citizenship = person['properties']['citizenship'][0].upper()
                birth_date = person['properties']['birthDate'][0]
                unique_id = person['id']
                # Now 'data' is a dictionary
                self.write({
                        'sex': gender,
                        'date_of_birth': birth_date,
                        'title': title,
                        'education': education,
                        'religion': religion,
                        'citizenship': citizenship,
                        'middle_name': middle_name,
                        'place_of_birth': birth_place,
                        'career_history':position})
            
            except:
                None