from pyspark.sql import SparkSession
import re
import os

spark = (
    SparkSession.builder
    .appName("DailyMedParser")
    .config("spark.executor.memory", "20g")
    .config("spark.driver.memory", "20g")
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "4040")
    .config("spark.driver.host", "0.0.0.0")
    .config("spark.executor.heartbeatInterval", "120s")
    .config("spark.network.timeout", "800s")
    .config("spark.rpc.askTimeout", "600s")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .config("spark.python.worker.reuse", "true")
    .enableHiveSupport()
    .getOrCreate())

DRUG_NAME_RE = re.compile(r'<h1>Label:.*?id="drug-label">([^<]+)', re.DOTALL)
SETID_FILE_RE = re.compile(r'setid_([0-9a-f-]+)')
SETID_HTML_RE = re.compile(r'setid=([0-9a-f-]+)')
PRODUCT_TYPE_RE = re.compile(
    r'<td[^>]*class="formLabel"[^>]*>\s*Product\s*Type\s*</td>\s*'
    r'<td[^>]*class="formItem"[^>]*>\s*([^<]+)\s*</td>',
    re.DOTALL | re.IGNORECASE
)
TABLES_RE = re.compile(r'<table[^>]*class="formTablePetite"[^>]*>.*?</table>', re.DOTALL | re.IGNORECASE)
INGREDIENT_RE = re.compile(r'<td class="formItem"><strong>([^<]+)</strong>', re.DOTALL | re.IGNORECASE)
STRENGTH_RE = re.compile(
    r'<td[^>]*class="formItem"[^>]*>\s*'
    r'((?:\d+(?:[.,]\d+)?[\u00A0\s]*[a-zA-Zμµu]+'
    r'(?:\s*(?:/|per|in)\s*\d*(?:[.,]\d+)?[\u00A0\s]*[a-zA-Zμµu]*)?)?)'
    r'\s*</td>',
    re.DOTALL | re.IGNORECASE
)
SECTIONS = {
    '34067-9': re.compile(r'<div[^>]*data-sectioncode="34067-9"[^>]*>(.*?)</div>\s*</li>', re.DOTALL | re.IGNORECASE),
    '34070-3': re.compile(r'<div[^>]*data-sectioncode="34070-3"[^>]*>(.*?)</div>\s*</li>', re.DOTALL | re.IGNORECASE),
    '34071-1': re.compile(r'<div[^>]*data-sectioncode="34071-1"[^>]*>(.*?)</div>\s*</li>', re.DOTALL | re.IGNORECASE)
}
TAG_RE = re.compile(r'<[^>]+>')
WS_RE = re.compile(r'\s+')

def parse_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            html = f.read().replace('\xa0', ' ')

        drug_match = DRUG_NAME_RE.search(html)
        if not drug_match:
            return None

        filename = os.path.basename(filepath)
        setid_match = SETID_FILE_RE.search(filename) or SETID_HTML_RE.search(html)
        if not setid_match:
            return None

        tables = TABLES_RE.findall(html)
        active = ''
        for table in tables:
            if 'active ingredient' in table.lower():
                ings = INGREDIENT_RE.findall(table)
                if ings:
                    strs = STRENGTH_RE.findall(table)
                    combined = []
                    for i, ing in enumerate(ings):
                        ing = WS_RE.sub(' ', ing).strip()
                        s = WS_RE.sub(' ', strs[i]).strip().lower() if i < len(strs) and strs[i].strip() else ''
                        combined.append(f"{ing} ({s})" if s else ing)
                    active = ', '.join(combined)
                break

        inactive = ''
        for table in tables:
            if 'inactive ingredient' in table.lower():
                ings = INGREDIENT_RE.findall(table)
                inactive = ', '.join([WS_RE.sub(' ', i).strip() for i in ings])
                break

        sections = []
        for code, regex in SECTIONS.items():
            match = regex.search(html)
            if match:
                text = TAG_RE.sub(' ', match.group(1))
                sections.append(WS_RE.sub(' ', text).strip())
            else:
                sections.append('')

        prod_match = PRODUCT_TYPE_RE.search(html)
        prod_type = WS_RE.sub(' ', prod_match.group(1)).strip().lower() if prod_match else ''
        
        return (
            setid_match.group(1),
            WS_RE.sub(' ', drug_match.group(1)).strip(),
            prod_type,
            active,
            inactive,
            sections[0],
            sections[1],
            sections[2],
            filepath
        )
    except Exception as e:
        return None

import glob
html_files = glob.glob("data/html/*/*")

files_rdd = spark.sparkContext.parallelize(html_files, numSlices=200)
parsed_rdd = files_rdd.map(parse_file).filter(lambda x: x is not None)

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("setid", StringType()),
    StructField("drug_name", StringType()),
    StructField("product_type", StringType()),
    StructField("active_ingredients", StringType()),
    StructField("inactive_ingredients", StringType()),
    StructField("indications_and_usage", StringType()),
    StructField("contraindications", StringType()),
    StructField("warnings", StringType()),
    StructField("filepath", StringType())
])

drugs_df = spark.createDataFrame(parsed_rdd, schema).dropDuplicates(["setid"])
drugs_df.write.mode("overwrite").csv("data/drugs.tsv", sep="\t", header=True)
print(f"{drugs_df.count()} drugs")