import lucene
from java.nio.file import Paths
from java.util import HashMap
from java.lang import Float
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser
import sys
import readline

lucene.initVM()

class DrugSearchCLI:
    def __init__(self, index_dir="data/lucene_index"):
        self.analyzer = StandardAnalyzer()
        self.directory = FSDirectory.open(Paths.get(index_dir))
        
        try:
            self.reader = DirectoryReader.open(self.directory)
            self.searcher = IndexSearcher(self.reader)
            self.stored_fields = self.reader.storedFields()
        except:
            print(f"Error: Index not found at {index_dir}")
            print("Please create the index first using pylucene_indexer.py")
            sys.exit(1)
    
    def search(self, query_str, fuzzy=False, top_k=10):
        fields = ["drug_name", "active_ingredients", "indications_and_usage", 
                  "medical_uses", "pharmacodynamics", "pharmacokinetics"]
        
        boosts = HashMap()
        boosts.put("drug_name", Float(5.0))
        boosts.put("active_ingredients", Float(4.0))
        boosts.put("indications_and_usage", Float(4.0))
        boosts.put("medical_uses", Float(2.0))
        boosts.put("pharmacodynamics", Float(1.5))
        boosts.put("pharmacokinetics", Float(1.0))
        
        
        parser = MultiFieldQueryParser(fields, self.analyzer, boosts)
        parser.setDefaultOperator(QueryParser.Operator.AND)
        
        if fuzzy:
            parser.setFuzzyMinSim(0.7)
            query_str = ' '.join([f"{term}~" for term in query_str.split()])
        
        try:
            query = QueryParser.parse(parser, query_str)
        except:
            parser.setDefaultOperator(QueryParser.Operator.OR)
            query = QueryParser.parse(parser, query_str)
        
        hits = self.searcher.search(query, top_k * 2).scoreDocs
        
        results = []
        seen = set()
        for hit in hits:
            doc = self.stored_fields.document(hit.doc)
            drug_name = doc.get('drug_name')
            
            if drug_name not in seen:
                seen.add(drug_name)
                results.append({
                    'score': hit.score,
                    'drug_name': drug_name,
                    'active_ingredients': doc.get('active_ingredients'),
                    'indications': doc.get('indications_and_usage'),
                    'warnings': doc.get('warnings'),
                    'setid': doc.get('setid')
                })
                
                if len(results) >= top_k:
                    break
        
        return results
    
    def display_results(self, results, show_details=False):
        if not results:
            print("No results found.")
            return
        
        print(f"\nFound {len(results)} results:\n")
        
        for i, r in enumerate(results, 1):
            print(f"{i}. {r['drug_name']}")
            print(f"   Score: {r['score']:.2f}")
            
            if show_details:
                print(f"   Active Ingredients: {r['active_ingredients'][:100]}...")
                if r['indications']:
                    print(f"   Indications: {r['indications'][:150]}...")
                print(f"   SetID: {r['setid']}")
            
            print()
    
    def interactive_mode(self):
        print("=" * 70)
        print("PyLucene Drug Search")
        print("=" * 70)
        print("\nCommands:")
        print("  - Enter query to search")
        print("  - 'fuzzy <query>' for fuzzy search")
        print("  - 'details <query>' for detailed results")
        print("  - 'quit' or 'exit' to quit")
        print()
        
        while True:
            try:
                user_input = input("Search> ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() in ['quit', 'exit', 'q']:
                    print("Goodbye!")
                    break
                
                fuzzy = False
                show_details = False
                
                if user_input.lower().startswith('fuzzy '):
                    fuzzy = True
                    query = user_input[6:].strip()
                elif user_input.lower().startswith('details '):
                    show_details = True
                    query = user_input[8:].strip()
                else:
                    query = user_input
                
                if not query:
                    print("Please enter a query.")
                    continue
                
                results = self.search(query, fuzzy=fuzzy)
                self.display_results(results, show_details=show_details)
                
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
    
    def close(self):
        if hasattr(self, 'reader'):
            self.reader.close()


def main():
    if len(sys.argv) > 1:
        cli = DrugSearchCLI()
        
        query = ' '.join(sys.argv[1:])
        fuzzy = '--fuzzy' in sys.argv
        
        if fuzzy:
            query = query.replace('--fuzzy', '').strip()
        
        print(f"Searching for: {query}")
        if fuzzy:
            print("(with fuzzy matching)")
        print()
        
        results = cli.search(query, fuzzy=fuzzy)
        cli.display_results(results, show_details=True)
        cli.close()
    else:
        cli = DrugSearchCLI()
        cli.interactive_mode()
        cli.close()


if __name__ == "__main__":
    main()
