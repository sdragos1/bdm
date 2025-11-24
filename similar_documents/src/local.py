import hashlib
import random
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import pandas as pd

# Configuration
DATA_DIR = "./similar_documents/data/"  # Local directory for Reuters files
OUTPUT_FILE = "./output/similar_documents.csv"  # Output CSV file

K_SHINGLE_SIZE = 5
NUM_HASHES = 100
NUM_BANDS = 20
ROWS_PER_BAND = NUM_HASHES // NUM_BANDS
SIMILARITY_THRESHOLD = 0.8

last_loaded_document_id: None | int = None


def parse_reuters_xml(xml_content):
    """Parse Reuters XML/SGML content and extract documents"""
    documents = []
    reuters_pattern = r'<REUTERS[^>]*>.*?</REUTERS>'
    doc_matches = re.findall(reuters_pattern, xml_content, re.DOTALL)

    for doc in doc_matches:
        try:
            doc = re.sub(r'&#\d+;', ' ', doc)
            root = ET.fromstring(doc)

            newid = root.get('NEWID', '')
            oldid = root.get('OLDID', '')

            date_elem = root.find('DATE')
            date = date_elem.text.strip() if date_elem is not None and date_elem.text else ''

            def get_list(tag_name):
                elem = root.find(tag_name)
                if elem is not None:
                    return [d.text.strip() for d in elem.findall('D') if d.text]
                return []

            places = get_list('PLACES')
            topics = get_list('TOPICS')
            people = get_list('PEOPLE')
            orgs = get_list('ORGS')

            title = ''
            body = ''
            dateline = ''

            text_elem = root.find('TEXT')
            if text_elem is not None:
                title_elem = text_elem.find('TITLE')
                if title_elem is not None and title_elem.text:
                    title = title_elem.text.strip()

                body_elem = text_elem.find('BODY')
                if body_elem is not None and body_elem.text:
                    body = body_elem.text.strip()

                dateline_elem = text_elem.find('DATELINE')
                if dateline_elem is not None and dateline_elem.text:
                    dateline = dateline_elem.text.strip()

                if body == '':
                    print(f"Empty body for document NEWID={newid}")

                documents.append({
                    'newid': newid,
                    'oldid': oldid,
                    'date': date,
                    'topics': topics,
                    'places': places,
                    'people': people,
                    'orgs': orgs,
                    'title': title,
                    'dateline': dateline,
                    'body': body,
                    'text': f"{title} {body}".strip()
                })
            else:
                print(newid)
        except:
            continue
    return documents


def generate_k_shingles(text, k):
    """Generate k-shingles from text"""
    if not text or len(text) < k:
        return []
    text = ' '.join(text.lower().split())
    return sorted(list(set(text[i:i + k] for i in range(len(text) - k + 1))))


def generate_minhash_signature(shingles, num_hashes):
    """Generate MinHash signature for a set of shingles"""
    if not shingles:
        return [2147483647] * num_hashes

    signature = []
    prime = 2147483647

    for i in range(num_hashes):
        random.seed(i)
        a = random.randint(1, prime - 1)
        b = random.randint(0, prime - 1)

        min_hash = prime
        for shingle in shingles:
            shingle_hash = int(hashlib.md5(shingle.encode()).hexdigest(), 16) % prime
            hash_value = (a * shingle_hash + b) % prime
            if hash_value < min_hash:
                min_hash = hash_value

        signature.append(int(min_hash))

    return signature


def generate_lsh_bands(signature, num_bands, rows_per_band):
    """Generate LSH bands from MinHash signature"""
    bands = []
    for i in range(num_bands):
        start_idx = i * rows_per_band
        end_idx = start_idx + rows_per_band
        band_values = tuple(signature[start_idx:end_idx])
        band_hash = hashlib.md5(str(band_values).encode()).hexdigest()
        bands.append((i, band_hash))
    return bands


def compute_jaccard_similarity(shingles1, shingles2):
    """Compute Jaccard similarity between two sets of shingles"""
    set1 = set(shingles1)
    set2 = set(shingles2)

    if len(set1) == 0 and len(set2) == 0:
        return 1.0

    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))

    return intersection / union if union > 0 else 0.0


def load_reuters_documents(data_dir):
    """Load all Reuters documents from local directory"""
    print(f"\nLoading Reuters files from {data_dir}...")
    all_documents = []
    data_path = Path(data_dir)

    if not data_path.exists():
        print(f"Error: Directory {data_dir} does not exist!")
        print(f"Please create the directory and place Reuters .sgm files there.")
        return []

    # Find all .sgm files
    sgm_files = sorted(data_path.glob("*.sgm"))

    if not sgm_files:
        print(f"Error: No .sgm files found in {data_dir}")
        return []

    total_docs = 0;
    for file_path in sgm_files:
        try:
            print(f"Loading {file_path.name}...", end=" ")

            with open(file_path, 'r', encoding='latin-1') as f:
                content = f.read()

            docs = parse_reuters_xml(content)
            all_documents.extend(docs)

            print(f"Last loaded document ID: {docs[-1]['newid'] if docs else 'N/A'} ", end="")
            print(f"({len(docs)} documents)")
            total_docs += len(docs)

        except Exception as e:
            print(f"Error: {e}")
            continue

    print(f"\nTotal documents: {total_docs}")
    print(f"\nTotal documents loaded: {len(all_documents)}")
    exit(0)
    return all_documents


def process_documents(documents):
    """Process documents: generate shingles, signatures, and LSH bands"""
    print("\nProcessing documents...")
    processed_docs = []

    for i, doc in enumerate(documents):
        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(documents)} documents...")

        if doc['text']:
            shingles = generate_k_shingles(doc['text'], K_SHINGLE_SIZE)
            if shingles:
                signature = generate_minhash_signature(shingles, NUM_HASHES)
                bands = generate_lsh_bands(signature, NUM_BANDS, ROWS_PER_BAND)
                processed_docs.append({
                    'doc_id': doc['newid'],
                    'title': doc['title'],
                    'shingles': shingles,
                    'signature': signature,
                    'bands': bands
                })

    print(f"Processed {len(processed_docs)} documents with shingles")
    return processed_docs


def find_candidate_pairs(processed_docs):
    """Use LSH to find candidate pairs"""
    print("\nFinding candidate pairs using LSH...")
    band_buckets = defaultdict(list)

    for doc in processed_docs:
        for band_id, band_hash in doc['bands']:
            band_buckets[(band_id, band_hash)].append(doc['doc_id'])

    candidate_pairs = set()
    for bucket_docs in band_buckets.values():
        if len(bucket_docs) > 1:
            for i in range(len(bucket_docs)):
                for j in range(i + 1, len(bucket_docs)):
                    doc_id1, doc_id2 = sorted([bucket_docs[i], bucket_docs[j]])
                    candidate_pairs.add((doc_id1, doc_id2))

    print(f"Found {len(candidate_pairs)} candidate pairs")
    return candidate_pairs


def compute_similarities(candidate_pairs, processed_docs, threshold):
    """Compute Jaccard similarity for candidate pairs"""
    print(f"\nComputing similarities for candidate pairs (threshold >= {threshold})...")

    doc_shingles_map = {doc['doc_id']: doc['shingles'] for doc in processed_docs}

    similar_pairs = []
    for i, (doc_id1, doc_id2) in enumerate(candidate_pairs):
        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(candidate_pairs)} pairs...")

        similarity = compute_jaccard_similarity(
            doc_shingles_map[doc_id1],
            doc_shingles_map[doc_id2]
        )
        if similarity >= threshold:
            similar_pairs.append({
                'doc_id_1': doc_id1,
                'doc_id_2': doc_id2,
                'similarity': float(similarity)
            })

    similar_pairs.sort(key=lambda x: x['similarity'], reverse=True)
    print(f"Found {len(similar_pairs)} similar pairs")
    return similar_pairs


def save_results(similar_pairs, output_file):
    """Save results to CSV file"""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(similar_pairs)
    df.to_csv(output_file, index=False, sep='\t')
    print(f"\nResults saved to {output_file}")


def main():
    """Main pipeline"""
    print("=" * 80)
    print("Reuters Similar Documents Finder - Local Version")
    print("=" * 80)

    # Load documents
    all_documents = load_reuters_documents(DATA_DIR)

    if not all_documents:
        print("\nNo documents loaded. Exiting.")
        return

    # Show sample documents
    print("\nSample documents:")
    for doc in all_documents[:5]:
        print(f"  {doc['newid']}: {doc['title'][:60]}...")

    # Process documents
    processed_docs = process_documents(all_documents)

    if not processed_docs:
        print("\nNo documents with shingles found. Exiting.")
        return

    # Find candidate pairs using LSH
    candidate_pairs = find_candidate_pairs(processed_docs)

    if not candidate_pairs:
        print(f"\nNo candidate pairs found. Try adjusting NUM_BANDS or SIMILARITY_THRESHOLD.")
        return

    # Compute similarities
    similar_pairs = compute_similarities(candidate_pairs, processed_docs, SIMILARITY_THRESHOLD)

    # Display results
    print("\n" + "=" * 80)
    print(f"Similar Document Pairs (Jaccard >= {SIMILARITY_THRESHOLD})")
    print("=" * 80)

    if similar_pairs:
        df = pd.DataFrame(similar_pairs)
        print(df.to_string(index=False))
        print(f"\nTotal similar pairs found: {len(similar_pairs)}")

        # Save results
        save_results(similar_pairs, OUTPUT_FILE)
    else:
        print("\nNo similar document pairs found")
        print(f"Try lowering SIMILARITY_THRESHOLD (current: {SIMILARITY_THRESHOLD})")

    print("\n" + "=" * 80)
    print("Pipeline completed!")
    print("=" * 80)


# Run the pipeline
if __name__ == "__main__":
    main()
