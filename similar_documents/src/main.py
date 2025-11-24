import argparse
import hashlib
import random
import re
import xml.etree.ElementTree as Element
from collections import defaultdict
from typing import List, Dict, Tuple, Iterator, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType

# HDFS configuration
HDFS_NAMENODE: str = "hdfs://192.168.0.12:9000"
DATA_DIR: str = f"{HDFS_NAMENODE}/input/similar_documents"
OUTPUT_DIR: str = f"{HDFS_NAMENODE}/output/similar_documents"


def parse_reuters_xml(xml_content: str) -> List[Dict[str, Any]]:
    documents: List[Dict[str, Any]] = []
    reuters_pattern: str = r'<REUTERS[^>]*>.*?</REUTERS>'
    doc_matches: List[str] = re.findall(reuters_pattern, xml_content, re.DOTALL)

    for document in doc_matches:
        try:
            document = re.sub(r'&#\d+;', ' ', document)
            root = Element.fromstring(document)

            newid: str = root.get('NEWID', '')
            oldid: str = root.get('OLDID', '')

            date_elem = root.find('DATE')
            date: str = date_elem.text.strip() if date_elem is not None and date_elem.text else ''

            def get_list(tag_name: str) -> List[str]:
                elem = root.find(tag_name)
                if elem is not None:
                    return [d.text.strip() for d in elem.findall('D') if d.text]
                return []

            places: List[str] = get_list('PLACES')
            topics: List[str] = get_list('TOPICS')
            people: List[str] = get_list('PEOPLE')
            orgs: List[str] = get_list('ORGS')

            title: str = ''
            body: str = ''
            dateline: str = ''

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
        except:
            continue

    return documents


def generate_k_shingles(text: str, k: int) -> List[str]:
    if not text or len(text) < k:
        return []
    text = ' '.join(text.lower().split())
    return sorted(list(set(text[i:i + k] for i in range(len(text) - k + 1))))


def generate_minhash_signature(shingles_list: List[str], num_hashes: int) -> List[int]:
    if not shingles_list:
        return [2147483647] * num_hashes

    signature: List[int] = []
    prime: int = 2147483647

    for i in range(num_hashes):
        random.seed(i)
        a: int = random.randint(1, prime - 1)
        b: int = random.randint(0, prime - 1)

        min_hash: int = prime
        for shg in shingles_list:
            shingle_hash: int = int(hashlib.md5(shg.encode()).hexdigest(), 16) % prime
            hash_value: int = (a * shingle_hash + b) % prime
            if hash_value < min_hash:
                min_hash = hash_value

        signature.append(int(min_hash))

    return signature


def generate_lsh_bands(signature: List[int], bands_len: int, rows_per_band: int, num_band_hashes: int) -> List[
    Tuple[int, str]]:
    bands: List[Tuple[int, str]] = []
    prime: int = 2147483647

    for band_idx in range(bands_len):
        start_idx: int = band_idx * rows_per_band
        end_idx: int = start_idx + rows_per_band
        band_values: Tuple[int, ...] = tuple(signature[start_idx:end_idx])

        band_hashes: List[int] = []
        for hash_index in range(num_band_hashes):
            random.seed(band_idx * 1000 + hash_index)
            a: int = random.randint(1, prime - 1)
            b: int = random.randint(0, prime - 1)

            band_str: str = str(band_values)
            band_int_hash: int = int(hashlib.md5(band_str.encode()).hexdigest(), 16) % prime
            hash_value: int = (a * band_int_hash + b) % prime
            band_hashes.append(hash_value)

        combined_hash: str = hashlib.md5(str(tuple(band_hashes)).encode()).hexdigest()
        bands.append((band_idx, combined_hash))

    return bands


def process_partition(documents_iter: Iterator[Dict[str, Any]], k: int, num_hashes: int, bands_len: int,
                      rows_per_band: int, num_band_hashes: int) -> Iterator[Dict[str, Any]]:
    processed: List[Dict[str, Any]] = []
    for document in documents_iter:
        if document['text']:
            shingles_list: List[str] = generate_k_shingles(document['text'], k)
            if shingles_list:
                signature: List[int] = generate_minhash_signature(shingles_list, num_hashes)
                bands: List[Tuple[int, str]] = generate_lsh_bands(signature, bands_len, rows_per_band, num_band_hashes)
                processed.append({
                    'doc_id': document['newid'],
                    'title': document['title'],
                    'shingles': shingles_list,
                    'signature': signature,
                    'bands': bands
                })
    return iter(processed)


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='LSH Similar Documents Pipeline')
    parser.add_argument('--k', type=int, default=5,
                        help='k-shingle size (default: 5)')
    parser.add_argument('--H', type=int, default=100,
                        help='Number of MinHash functions (default: 100)')
    parser.add_argument('--b', type=int, default=20,
                        help='Number of bands (default: 20)')
    parser.add_argument('--r', type=int, default=None,
                        help='Number of rows per band (default: H/b)')
    parser.add_argument('--band-hashes', type=int, default=1,
                        help='Number of hash functions for hashing each band (default: 1)')

    args = parser.parse_args()

    # Set parameters
    K_SHINGLE_SIZE = args.k
    NUM_HASHES = args.H
    NUM_BANDS = args.b

    # Calculate rows per band
    if args.r is not None:
        ROWS_PER_BAND = args.r
        if NUM_HASHES != NUM_BANDS * ROWS_PER_BAND:
            print(f"Warning: H ({NUM_HASHES}) != b ({NUM_BANDS}) * r ({ROWS_PER_BAND})")
            print(f"Using r={ROWS_PER_BAND}, but H should be {NUM_BANDS * ROWS_PER_BAND}")
    else:
        ROWS_PER_BAND = NUM_HASHES // NUM_BANDS
        if NUM_HASHES % NUM_BANDS != 0:
            print(f"Warning: H ({NUM_HASHES}) is not divisible by b ({NUM_BANDS})")
            print(
                f"Using r={ROWS_PER_BAND}, which means {NUM_HASHES - NUM_BANDS * ROWS_PER_BAND} hash functions will be unused")

    NUM_BAND_HASHES = args.band_hashes

    print("=" * 80)
    print("LSH Similar Documents Pipeline - Configuration")
    print("=" * 80)
    print(f"k (k-shingle size): {K_SHINGLE_SIZE}")
    print(f"H (number of MinHash functions): {NUM_HASHES}")
    print(f"b (number of bands): {NUM_BANDS}")
    print(f"r (rows per band): {ROWS_PER_BAND}")
    print(f"Number of hash functions per band: {NUM_BAND_HASHES}")
    print(f"Total signature length: {NUM_BANDS * ROWS_PER_BAND}")
    print("=" * 80)

    spark = SparkSession.builder.appName("Reuters Similar Documents").master(
        "spark://192.168.0.2:7077").getOrCreate()

    sc = spark.sparkContext

    print("\nLoading Reuters files from HDFS...")
    all_documents = []

    for i in range(22):
        file_name = f"reut2-{i:03d}.sgm"
        file_path = f"{DATA_DIR}/{file_name}"

        try:
            print(f"Loading {file_name}...", end=" ")

            file_rdd = sc.textFile(file_path)
            content = "\n".join(file_rdd.collect())

            docs = parse_reuters_xml(content)
            all_documents.extend(docs)

            print(f"({len(docs)} documents)")

        except Exception as e:
            print(f"Error: {e}")
            continue

    print(f"\nTotal documents loaded: {len(all_documents)}")

    if not all_documents:
        print("No documents loaded. Exiting.")
        spark.stop()
        exit(1)

    print("\nSample documents:")
    for doc in all_documents[:5]:
        print(f"  {doc['newid']}: {doc['title'][:60]}...")

    docs_rdd = sc.parallelize(all_documents, numSlices=8)

    params = (K_SHINGLE_SIZE, NUM_HASHES, NUM_BANDS, ROWS_PER_BAND, NUM_BAND_HASHES)
    params_bc = sc.broadcast(params)


    def process_with_params(docs_iter):
        k, num_h, num_b, r, num_bh = params_bc.value
        return process_partition(docs_iter, k, num_h, num_b, r, num_bh)


    processed_rdd = docs_rdd.mapPartitions(process_with_params)
    processed_docs = processed_rdd.collect()

    params_bc.destroy()

    if not processed_docs:
        print("No documents with shingles found. Exiting.")
        spark.stop()
        exit(1)

    print(f"\nProcessing {len(processed_docs)} documents...")

    doc_shingles_map = {doc['doc_id']: doc['shingles'] for doc in processed_docs}
    doc_signatures_map = {doc['doc_id']: doc['signature'] for doc in processed_docs}

    all_shingles = set()
    for shingles in doc_shingles_map.values():
        all_shingles.update(shingles)
    all_shingles = sorted(list(all_shingles))

    print(f"Total unique shingles: {len(all_shingles)}")
    print(f"Total documents: {len(processed_docs)}")
    print(f"Number of hash functions (H): {NUM_HASHES}")
    print(f"Number of bands (b): {NUM_BANDS}")
    print(f"Rows per band (r): {ROWS_PER_BAND}")

    print("\n" + "=" * 80)
    print("Generating Set Representation (MxN matrix)...")
    print("=" * 80)

    set_representation = []
    for shingle in all_shingles:
        for doc_id in doc_shingles_map.keys():
            value = 1 if shingle in doc_shingles_map[doc_id] else 0
            if value == 1:
                set_representation.append({
                    'shingle': shingle,
                    'doc_id': doc_id,
                    'value': str(value)
                })

    set_rep_schema = StructType([
        StructField("shingle", StringType(), False),
        StructField("doc_id", StringType(), False),
        StructField("value", StringType(), False)
    ])

    set_rep_df = spark.createDataFrame(set_representation, set_rep_schema)
    set_rep_path = f"{OUTPUT_DIR}/set_representation"
    print(f"Saving set representation to {set_rep_path}...")
    set_rep_df.coalesce(1).write.mode("overwrite").csv(
        set_rep_path,
        header=True,
        sep=","
    )
    print(f"Set representation saved: {len(set_representation)} entries")

    print("\n" + "=" * 80)
    print("Generating MinHash Signatures (HxN matrix)...")
    print("=" * 80)

    signature_matrix = []
    for hash_idx in range(NUM_HASHES):
        for doc_id in doc_signatures_map.keys():
            signature_value = doc_signatures_map[doc_id][hash_idx]
            signature_matrix.append({
                'hash_function_index': str(hash_idx),
                'doc_id': doc_id,
                'signature_value': str(signature_value)
            })

    sig_schema = StructType([
        StructField("hash_function_index", StringType(), False),
        StructField("doc_id", StringType(), False),
        StructField("signature_value", StringType(), False)
    ])

    sig_df = spark.createDataFrame(signature_matrix, sig_schema)
    sig_path = f"{OUTPUT_DIR}/minhash_signatures"
    print(f"Saving MinHash signatures to {sig_path}...")
    sig_df.coalesce(1).write.mode("overwrite").csv(
        sig_path,
        header=True,
        sep=","
    )
    print(f"MinHash signatures saved: {len(signature_matrix)} entries")

    print("\n" + "=" * 80)
    print("Finding Candidate Pairs (same bucket in more than one band)...")
    print("=" * 80)

    pair_band_count = defaultdict(int)

    band_buckets = defaultdict(list)
    for doc in processed_docs:
        for band_id, band_hash in doc['bands']:
            band_buckets[(band_id, band_hash)].append(doc['doc_id'])

    for bucket_docs in band_buckets.values():
        if len(bucket_docs) > 1:
            for i in range(len(bucket_docs)):
                for j in range(i + 1, len(bucket_docs)):
                    doc_id1, doc_id2 = sorted([bucket_docs[i], bucket_docs[j]])
                    pair_band_count[(doc_id1, doc_id2)] += 1

    candidate_pairs = []
    for (doc_id1, doc_id2), num_bands in pair_band_count.items():
        if num_bands > 1:
            candidate_pairs.append({
                'doc_id_1': doc_id1,
                'doc_id_2': doc_id2,
                'num_bands': str(num_bands)
            })

    candidate_pairs.sort(key=lambda x: x['num_bands'], reverse=True)

    if not candidate_pairs:
        print("No candidate pairs found (pairs in more than one band).")
        print("Try adjusting --b (number of bands) or --H (number of hash functions).")
    else:
        candidate_schema = StructType([
            StructField("doc_id_1", StringType(), False),
            StructField("doc_id_2", StringType(), False),
            StructField("num_bands", StringType(), False)
        ])

        candidate_df = spark.createDataFrame(candidate_pairs, candidate_schema)
        candidate_path = f"{OUTPUT_DIR}/candidate_pairs"
        print(f"Saving candidate pairs to {candidate_path}...")
        candidate_df.coalesce(1).write.mode("overwrite").csv(
            candidate_path,
            header=True,
            sep=","
        )
        print(f"Candidate pairs saved: {len(candidate_pairs)} pairs")
        print(f"  (pairs appearing in 2+ bands: {len(candidate_pairs)})")
        candidate_df.show(20, truncate=False)

    print("\n" + "=" * 80)
    print("Pipeline completed!")
    print("=" * 80)
    print(f"\nOutput files saved to HDFS:")
    print(f"  1. Set Representation: {OUTPUT_DIR}/set_representation")
    print(f"  2. MinHash Signatures: {OUTPUT_DIR}/minhash_signatures")
    print(f"  3. Candidate Pairs: {OUTPUT_DIR}/candidate_pairs")
    print("=" * 80)

    spark.stop()
