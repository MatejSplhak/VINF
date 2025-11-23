import re
import csv
from pathlib import Path
import os

class DailyMedParser:
    def __init__(self,output_dir='data'):
        self.drug_count = 0
        self.output_dir = Path(output_dir)
        self.tsv_path = self.output_dir / 'drugs.tsv'
        self.tsv_file = open(self.tsv_path, 'w', newline='', encoding='utf-8')
        self.tsv_writer = csv.writer(self.tsv_file, delimiter='\t')
        self.tsv_writer.writerow([
            'setid', 'drug_name',"product_type",'active_ingredients', 'inactive_ingredients','indications_and_usage','contraindications', 'warnings',
            'filepath'
        ])
        self.parsed = set()

    def extract_text_between(self, html, start_pattern, end_pattern):
            match = re.search(f'{start_pattern}(.*?){end_pattern}', html, re.DOTALL | re.IGNORECASE)
            if match:
                text = match.group(1)
                text = re.sub(r'<[^>]+>', ' ', text)
                text = re.sub(r'\s+', ' ', text).strip()
                return text[:5000]
            return ''

    def extract_table_ingredients(self, html, heading_text):
        tables = re.findall(
            r'<table[^>]*class="formTablePetite"[^>]*>.*?</table>',
            html, re.DOTALL | re.IGNORECASE
        )

        table_html = ''
        for t in tables:
            if re.search(re.escape(heading_text), t, re.IGNORECASE):
                table_html = t
                break
        if not table_html:
            return ''

        ingredient_pattern = r'<td class="formItem"><strong>([^<]+)</strong>'
        ingredients = re.findall(ingredient_pattern, table_html, re.DOTALL | re.IGNORECASE)
        ingredients = [re.sub(r'\s+', ' ', i.strip()) for i in ingredients]

        if heading_text.lower() == "active ingredient/active moiety".lower():
            strength_pattern = (
                r'<td[^>]*class="formItem"[^>]*>\s*'
                r'((?:\d+(?:[.,]\d+)?[\u00A0\s]*[a-zA-Zμµu]+'
                r'(?:\s*(?:/|per|in)\s*\d*(?:[.,]\d+)?[\u00A0\s]*[a-zA-Zμµu]*)?)?)'
                r'\s*</td>'
            )
            strengths = re.findall(strength_pattern, table_html, re.DOTALL | re.IGNORECASE)
            strengths = [re.sub(r'\s+', ' ', s.strip().lower()) for s in strengths if s.strip()]

            combined = []
            for i, ing in enumerate(ingredients):
                strength = strengths[i] if i < len(strengths) else ''
                combined.append(f"{ing} ({strength})" if strength else ing)

            return ', '.join(combined)

        return ', '.join(ingredients)
    

    def extract_by_section_code(self, html, section_code):

        pattern = rf'<div[^>]*data-sectioncode="{section_code}"[^>]*>(.*?)</div>\s*</li>'
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            text = match.group(1)
            text = re.sub(r'<[^>]+>', ' ', text) 
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        return ''
    
    def get_setid(self, html,filename):
        file_components = filename.split("_")
        if "setid" in file_components:
            setid = file_components[file_components.index("setid")+1]
            return setid.split(".")[0]
        else:
            setid = re.search(r'setid=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', html)
            if setid:
                setid = setid.group(1)
                return setid
            
    def extract_product_type(self, html):
        pattern = (
            r'<td[^>]*class="formLabel"[^>]*>\s*Product\s*Type\s*</td>\s*'
            r'<td[^>]*class="formItem"[^>]*>\s*([^<]+)\s*</td>'
        )
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            product_type = match.group(1)
            product_type = re.sub(r'\s+', ' ', product_type).strip().lower()
            return product_type
        return ''

    def parse_drug_detail(self, html, filename,filepath):
        html = html.replace('\xa0', ' ')
        drug_name_match = re.search(r'<h1>Label:.*?id="drug-label">([^<]+)', html, re.DOTALL)
        if not drug_name_match:
            return
        setid = self.get_setid(html,filename)
        
        drug_name = drug_name_match.group(1).strip() if drug_name_match else None
        active_ingredients = self.extract_table_ingredients(html, "Active Ingredient/Active Moiety")
        inactive_ingredients = self.extract_table_ingredients(html, "Inactive Ingredients")
        indications_and_usage = self.extract_by_section_code(html, "34067-9")
        contraindications = self.extract_by_section_code(html, "34070-3")
        warnings = self.extract_by_section_code(html, "34071-1")
        product_type = self.extract_product_type(html)

        data = {
            'setid': setid,
            'drug_name': drug_name,
            'product_type': product_type,
            'active_ingredients': active_ingredients,
            'inactive_ingredients': inactive_ingredients,
            'indications_and_usage': indications_and_usage,
            'contraindications':contraindications,
            'warnings': warnings,
            'filepath': filepath
        }

        self.tsv_writer.writerow(data.values())
        self.drug_count += 1
        return data
    
    def parse_batch(self, batch_name):
        batch_dir = self.output_dir / "html" / batch_name
        for f in os.listdir(batch_dir):
            filename = f
            filepath = batch_dir / filename
            with open(filepath, 'r', encoding='utf-8') as f:
                html = f.read()
                self.parse_drug_detail(html,filename,filepath)
            
        
def main():        
    parser = DailyMedParser()
    batches = os.listdir("data/html/")
    for batch in batches:
        parser.parse_batch(batch)

if __name__ == '__main__':
    main()