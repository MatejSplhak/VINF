import csv
import re
import json
from collections import defaultdict, Counter
import tiktoken

class DrugTFIDFIndexer:
    def __init__(self):
        self.index = defaultdict(dict)
        self.doc_count = 0
        self.doc_lengths = {}
        self.drugs = {}

        self.GENERAL_STOPWORDS = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 
            'for', 'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 
            'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'should', 'could', 'may', 'might', 'can', 'this',
            'that', 'these', 'those', 'it', 'its', 'you', 'your', 'not', 'who','any'
        }
        
        self.DOMAIN_STOPWORDS = {
            'drug', 'drugs', 'medicine', 'medication', 'medications',
            'treatment', 'pharmaceutical', 'therapy', 'oral', 'patient', 
            'patients', 'may', 'should', 'can', 'use', 'used', 'using'
        }
        
        self.ALL_STOPWORDS = self.GENERAL_STOPWORDS | self.DOMAIN_STOPWORDS
    
    def tokenize(self, text):
        if not text or text == 'Not found':
            return []

        text = text.lower()
        
        tokens = []

        dosage_pattern = r'\b\d+(?:\.\d+)?\s*(?:mg|mcg|g|ml|mcl|ug|iu|units?)\b(?:/\w+)?'
        dosages = re.findall(dosage_pattern, text)

        dosages = [re.sub(r'\s+', '', d) for d in dosages]
        tokens.extend(dosages)

        text = re.sub(dosage_pattern, ' ', text)

        compound_pattern = r'\b[a-z]+(?:-[a-z]+)+\b'
        compounds = re.findall(compound_pattern, text)
        tokens.extend(compounds)
        
        percentages = re.findall(r'\b\d+(?:\.\d+)?%\b', text)
        tokens.extend(percentages)

        words = re.findall(r'\b[a-z]{3,}\b', text)
        tokens.extend(words)

        filtered_tokens = []
        for token in tokens:

            if any(char.isdigit() for char in token) or '-' in token or '%' in token:
                filtered_tokens.append(token)

            elif token not in self.ALL_STOPWORDS and len(token) > 2:
                filtered_tokens.append(token)
        
        return filtered_tokens
    
    def create_document_text(self, drug_record, field_weights=None):

        if field_weights is None:
            field_weights = {
                'drug_name': 5,           
                'active_ingredients': 3,   
                'indications_and_usage': 4,
                'inactive_ingredients': 1,
                'contraindications': 1,   
                'warnings': 1              
            }
        
        text_parts = []
        for field, weight in field_weights.items():
            value = drug_record.get(field, '')
            if value and value != 'Not found':
                text_parts.extend([str(value)] * weight)
        
        return ' '.join(text_parts)
    
    def add_document(self, doc_id, text):
        tokens = self.tokenize(text)
        term_counts = Counter(tokens)
        
        for term, count in term_counts.items():
            self.index[term][doc_id] = count
        
        self.doc_lengths[doc_id] = len(tokens)
        self.doc_count += 1
    
    def load_from_tsv(self, tsv_path):
        
        with open(tsv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            
            for i, row in enumerate(reader):
                setid = row['setid']
                self.drugs[setid] = row
                doc_text = self.create_document_text(row)
                self.add_document(setid, doc_text)
                
                if (i + 1) % 1000 == 0:
                    print(f"  Indexed {i + 1} drugs...")
        
        print(f"\n Indexed {self.doc_count} drugs")
        print(f"Index contains {len(self.index)} unique terms")
    
    def save_index(self, output_path):
        data = {
            'index': {term: dict(docs) for term, docs in self.index.items()},
            'doc_count': self.doc_count,
            'doc_lengths': self.doc_lengths,
            'drugs': self.drugs
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        print(f"Index saved to {output_path}")
    
    def load_index(self, input_path):
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.index = defaultdict(dict, {
            term: docs for term, docs in data['index'].items()
        })
        self.doc_count = data['doc_count']
        self.doc_lengths = data['doc_lengths']
        self.drugs = data['drugs']
        
        print(f"Loaded index with {self.doc_count} documents")
    
    def get_statistics(self):
        total_tokens = sum(self.doc_lengths.values())
        avg_doc_length = total_tokens / self.doc_count if self.doc_count > 0 else 0

        term_doc_counts = {
            term: len(docs) for term, docs in self.index.items()
        }
        most_common = sorted(
            term_doc_counts.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:20]
        
        stats = {
            'total_documents': self.doc_count,
            'unique_terms': len(self.index),
            'total_tokens': total_tokens,
            'avg_document_length': avg_doc_length,
            'most_common_terms': most_common
        }
        
        return stats
    
    def get_tiktoken_statistics(self):
        
        encoding = tiktoken.get_encoding("cl100k_base")
        
        total_tiktoken_tokens = 0

        for i, (setid, drug) in enumerate(self.drugs.items()):
            doc_text = self.create_document_text(drug, field_weights={
                'drug_name': 1,
                'active_ingredients': 1,
                'inactive_ingredients': 1,
                'indications_and_usage': 1,
                'contraindications': 1,
                'warnings': 1
            })
            
            tokens = encoding.encode(doc_text)
            total_tiktoken_tokens += len(tokens)
    
        return {
            'total_tiktoken_tokens': total_tiktoken_tokens,
            'avg_tiktoken_tokens_per_doc': total_tiktoken_tokens / self.doc_count if self.doc_count > 0 else 0
        }
    def print_statistics(self):
        stats = self.get_statistics()
        
        print("\n" + "==================")
        print("INDEX STATISTICS")
        print("==================")
        print(f"Total Documents: {stats['total_documents']:,}")
        print(f"Unique Terms: {stats['unique_terms']:,}")
        print(f"Total Tokens: {stats['total_tokens']:,}")
        print(f"Average Document Length: {stats['avg_document_length']:.1f} tokens")
        
        print("\nMost Common Terms (by document frequency):")
        for term, doc_freq in stats['most_common_terms']:
            pct = (doc_freq / stats['total_documents']) * 100
            print(f"  {term:20s} : {doc_freq:5d} docs ({pct:5.1f}%)")

def main():

    indexer = DrugTFIDFIndexer()
    indexer.load_from_tsv('data/drugs.tsv')
    indexer.load_index('data/drug_index.json')
    indexer.print_statistics()
    print(indexer.get_tiktoken_statistics())
    indexer.save_index('data/drug_index.json')

if __name__ == "__main__":
    main()
