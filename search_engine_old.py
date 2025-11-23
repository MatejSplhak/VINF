import math
from collections import defaultdict
from indexer import DrugTFIDFIndexer

class IDFCalculator:
    
    def __init__(self, index, doc_count):
        self.index = index
        self.N = doc_count
    
    def standard_idf(self, term):
        df = len(self.index.get(term, {}))
        if df == 0:
            return 0
        return math.log(self.N / df)
    
    def smooth_idf(self, term):
        df = len(self.index.get(term, {}))
        return math.log(self.N / (df + 1))
    
    def probabilistic_idf(self, term):
        df = len(self.index.get(term, {}))
        if df == 0 or df == self.N:
            return 0
        return math.log((self.N - df) / df)
    
    def bm25_idf(self, term):
        df = len(self.index.get(term, {}))
        return math.log((self.N - df + 0.5) / (df + 0.5))


class DrugSearchEngine:
    
    def __init__(self, indexer):
        self.indexer = indexer
        self.idf_calc = IDFCalculator(indexer.index, indexer.doc_count)
    
    def search(self, query, idf_method='standard', top_k=10):

        query_terms = self.indexer.tokenize(query)
        
        if not query_terms:
            return []
        
        doc_term_counts = defaultdict(int)
        scores = defaultdict(float)

        for term in query_terms:
            if term not in self.indexer.index:
                continue

            if idf_method == 'standard':
                idf = self.idf_calc.standard_idf(term)
            elif idf_method == 'smooth':
                idf = self.idf_calc.smooth_idf(term)
            elif idf_method == 'probabilistic':
                idf = self.idf_calc.probabilistic_idf(term)
            elif idf_method == 'bm25':
                idf = self.idf_calc.bm25_idf(term)
            else:
                idf = self.idf_calc.standard_idf(term)

            for doc_id, tf in self.indexer.index[term].items():
                scores[doc_id] += tf * idf
                doc_term_counts[doc_id] += 1

        num_query_terms = len([t for t in query_terms if t in self.indexer.index])
        
        filtered_scores = {
            doc_id: score 
            for doc_id, score in scores.items() 
            if doc_term_counts[doc_id] == num_query_terms
        }
        
        if not filtered_scores:
            return []

        ranked = sorted(filtered_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        
        results = []
        for doc_id, score in ranked:
            drug = self.indexer.drugs[doc_id]
            results.append({
                'setid': doc_id,
                'score': score,
                'drug_name': drug['drug_name'],
                'indications': drug['indications_and_usage'][:200] + '...',
                'active_ingredients': drug['active_ingredients']
            })
        
        return results
        
    def compare_idf_methods(self, query, top_k=5):
        methods = ['standard', 'smooth', 'probabilistic', 'bm25']
        
        print(f"\nQuery: '{query}'")
        print("=======================")
        
        for method in methods:
            print(f"\n{method.upper()} IDF:")
            results = self.search(query, idf_method=method, top_k=20)
            printed_results = {"scores":[],"actives":[]}
            print_counter = 1
            for i, result in enumerate(results, 1):
                if print_counter <= top_k:
                    if not printed_results["scores"]:
                        print(f"  {print_counter}. {result['drug_name']}")
                        print(f"     Score: {result['score']:.4f}")
                        print(f"     Active: {result['active_ingredients'][:80]}")
                        print(f"     Usage: {result['indications'][:150]}")
                        printed_results["scores"].append(result["score"])
                        printed_results["actives"].append(result["active_ingredients"])
                        print_counter += 1
                    elif (result['score'] not in printed_results["scores"] and result['active_ingredients'] not in  printed_results["actives"]):
                        print(f"  {print_counter}. {result['drug_name']}")
                        print(f"     Score: {result['score']:.4f}")
                        print(f"     Active: {result['active_ingredients'][:80]}")
                        print(f"     Usage: {result['indications'][:150]}")
                        printed_results["scores"].append(result["score"])
                        printed_results["actives"].append(result["active_ingredients"])
                        print_counter += 1
                    
                    
def main():
    indexer = DrugTFIDFIndexer()
    indexer.load_index("data/drug_index.json")
    search_engine = DrugSearchEngine(indexer)

    test_queries = [
        "headache pain",
        "blood pressure hypertension",
        "allergy nasal congestion"
    ]
    
    print("\n" + "==========")
    print("COMPARING IDF METHODS")
    print("============")
    
    for query in test_queries:
        search_engine.compare_idf_methods(query, top_k=3)
        print("\n" + "------------------")
        
    
if __name__ == "__main__":
    main()
