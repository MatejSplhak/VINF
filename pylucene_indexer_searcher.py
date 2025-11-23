import lucene
from java.nio.file import Paths
from java.util import HashMap
from java.lang import Float
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, TextField, StringField, StoredField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser
import csv
import re

lucene.initVM()

def preprocess_text(text):
    if not text:
        return ""
    text = text.lower()
    dosage_pattern = r'\d+(?:\.\d+)?\s*(?:mg|mcg|g|ml|ug|iu|units?)\b'
    dosages = re.findall(dosage_pattern, text)
    for d in dosages:
        clean = re.sub(r'\s+', '', d)
        text = text.replace(d, clean)
    return text

class PyLuceneDrugIndexer:
    def __init__(self, index_dir="data/lucene_index"):
        self.index_dir = index_dir
        self.analyzer = StandardAnalyzer()
        self.directory = FSDirectory.open(Paths.get(index_dir))
    
    def index_exists(self):
        try:
            DirectoryReader.open(self.directory).close()
            return True
        except:
            return False
        
    def create_index(self, tsv_path):
        config = IndexWriterConfig(self.analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        writer = IndexWriter(self.directory, config)
        
        with open(tsv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            
            for i, row in enumerate(reader):
                doc = Document()
                doc.add(StringField("setid", row.get('setid', ''), Field.Store.YES))
                doc.add(TextField("drug_name", preprocess_text(row.get('drug_name', '')), Field.Store.YES))
                doc.add(TextField("active_ingredients", preprocess_text(row.get('active_ingredients', '')), Field.Store.YES))
                doc.add(TextField("indications_and_usage", preprocess_text(row.get('indications_and_usage', '')), Field.Store.YES))
                doc.add(TextField("contraindications", preprocess_text(row.get('contraindications', '')), Field.Store.YES))
                doc.add(TextField("warnings", preprocess_text(row.get('warnings', '')), Field.Store.YES))
                doc.add(TextField("pharmacodynamics", preprocess_text(row.get('pharmacodynamics', '')), Field.Store.YES))
                doc.add(TextField("pharmacokinetics", preprocess_text(row.get('pharmacokinetics', '')), Field.Store.YES))
                doc.add(TextField("medical_uses", preprocess_text(row.get('medical_uses', '')), Field.Store.YES))
                doc.add(TextField("adverse_effects", preprocess_text(row.get('adverse_effects', '')), Field.Store.YES))
                doc.add(StoredField("filepath", row.get('filepath', '')))
                
                writer.addDocument(doc)
                
                if (i + 1) % 1000 == 0:
                    print(f"  Indexed {i + 1} drugs...")
        
        writer.commit()
        writer.close()
        print(f"\n indexed {i + 1} drugs")
    
    def multi_field_search_fuzzy(self, query_str, top_k=15):
        reader = DirectoryReader.open(self.directory)
        searcher = IndexSearcher(reader)
        stored_fields = reader.storedFields()
        
        fields = ["drug_name", "active_ingredients", "indications_and_usage", 
                  "medical_uses", "pharmacodynamics", "pharmacokinetics",
                  "contraindications", "warnings", "adverse_effects"]
        
        boosts = HashMap()
        boosts.put("drug_name", Float(5.0))
        boosts.put("active_ingredients", Float(4.0))
        boosts.put("indications_and_usage", Float(4.0))
        boosts.put("medical_uses", Float(4.0))
        boosts.put("pharmacodynamics", Float(1.5))
        boosts.put("pharmacokinetics", Float(1.0))
        boosts.put("contraindications", Float(1.0))
        boosts.put("warnings", Float(1.0))
        boosts.put("adverse_effects", Float(1.0))
        
        parser = MultiFieldQueryParser(fields, self.analyzer, boosts)
        parser.setDefaultOperator(QueryParser.Operator.AND)
        parser.setFuzzyMinSim(0.7)
        
        try:
            fuzzy_query = ' '.join([f"{term}~" for term in query_str.split()])
            query = QueryParser.parse(parser, fuzzy_query)
        except:
            parser.setDefaultOperator(QueryParser.Operator.OR)
            query = QueryParser.parse(parser, fuzzy_query)
        
        hits = searcher.search(query, top_k).scoreDocs
        
        results = []
        for hit in hits:
            doc = stored_fields.document(hit.doc)
            results.append({
                'score': hit.score,
                'drug_name': doc.get('drug_name'),
                'active_ingredients': doc.get('active_ingredients'),
                'indications': doc.get('indications_and_usage')[:200] if doc.get('indications_and_usage') else ''
            })
        
        reader.close()
        return results
    
    def multi_field_search(self, query_str, top_k=15):
        reader = DirectoryReader.open(self.directory)
        searcher = IndexSearcher(reader)
        stored_fields = reader.storedFields()
        
        fields = ["drug_name", "active_ingredients", "indications_and_usage", 
                  "medical_uses", "pharmacodynamics", "pharmacokinetics",
                  "contraindications", "warnings", "adverse_effects"]
        
        boosts = HashMap()
        boosts.put("drug_name", Float(5.0))
        boosts.put("active_ingredients", Float(4.0))
        boosts.put("indications_and_usage", Float(4.0))
        boosts.put("medical_uses", Float(4.0))
        boosts.put("pharmacodynamics", Float(1.5))
        boosts.put("pharmacokinetics", Float(1.0))
        boosts.put("contraindications", Float(1.0))
        boosts.put("warnings", Float(1.0))
        boosts.put("adverse_effects", Float(1.0))
        
        parser = MultiFieldQueryParser(fields, self.analyzer, boosts)
        parser.setDefaultOperator(QueryParser.Operator.AND)
        
        try:
            query = QueryParser.parse(parser, query_str)
        except:
            parser.setDefaultOperator(QueryParser.Operator.OR)
            query = QueryParser.parse(parser, query_str)
        
        hits = searcher.search(query, top_k).scoreDocs
        
        results = []
        for hit in hits:
            doc = stored_fields.document(hit.doc)
            results.append({
                'score': hit.score,
                'drug_name': doc.get('drug_name'),
                'active_ingredients': doc.get('active_ingredients'),
                'indications': doc.get('indications_and_usage')[:200] if doc.get('indications_and_usage') else ''
            })
        
        reader.close()
        return results


def compare_query(old_indexer, new_indexer, query, top_k=5):
    print(f"\nQuery: '{query}'")
    print("===============")
    
    print("\nOLD TF-IDF INDEX")
    old_results = old_indexer.search(query, top_k=50)
    seen_old = set()
    count = 0
    for r in old_results:
        if r['drug_name'] not in seen_old:
            seen_old.add(r['drug_name'])
            count += 1
            print(f"{count}. {r['drug_name']} (score: {r['score']:.4f})")
            if count >= top_k:
                break
    
    print("\nPYLUCENE INDEX - Standard")
    new_results = new_indexer.multi_field_search(query, top_k=50)
    seen_new = set()
    count = 0
    for r in new_results:
        if r['drug_name'] not in seen_new:
            seen_new.add(r['drug_name'])
            count += 1
            print(f"{count}. {r['drug_name']} (score: {r['score']:.4f})")
            if count >= top_k:
                break
    
    print("\nPYLUCENE INDEX - Fuzzy AND")
    fuzzy_results = new_indexer.multi_field_search_fuzzy(query, top_k=50)
    seen_fuzzy = set()
    count = 0
    for r in fuzzy_results:
        if r['drug_name'] not in seen_fuzzy:
            seen_fuzzy.add(r['drug_name'])
            count += 1
            print(f"{count}. {r['drug_name']} (score: {r['score']:.4f})")
            if count >= top_k:
                break


def compare_multiple_queries(old_indexer, new_indexer, queries):
    for query in queries:
        compare_query(old_indexer, new_indexer, query)
        print("\n" + "-------------")


def main():
    pylucene_indexer = PyLuceneDrugIndexer()
    
    if pylucene_indexer.index_exists():
        print("index exists, loading..")
    else:
        print("creating pylucene index..")
        pylucene_indexer.create_index('data/wiki_drugs.tsv')

    from indexer import DrugTFIDFIndexer
    from search_engine import DrugSearchEngine
    old_indexer = DrugTFIDFIndexer()
    old_indexer.load_index('data/drug_index.json')
    old_search = DrugSearchEngine(old_indexer)

    test_queries = [
        "headache pain relief",
        "blood pressure hypertension",
        "diabtes treatment"
    ]
    compare_multiple_queries(old_search, pylucene_indexer, test_queries)


if __name__ == "__main__":
    main()