import argparse
import hashlib
import random
import re
import xml.etree.ElementTree as Element
from collections import defaultdict
from typing import List, Dict, Tuple, Iterator, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, IntegerType
from pyspark.sql.functions import col, count as spark_count, collect_list, explode, array, lit

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

            title: str = ''
            body: str = ''

            text_elem = root.find('TEXT')
            if text_elem is not None:
                title_elem = text_elem.find('TITLE')
                if title_elem is not None and title_elem.text:
                    title = title_elem.text.strip()

                body_elem = text_elem.find('BODY')
                if body_elem is not None and body_elem.text:
                    body = body_elem.text.strip()

            text = f"{title} {body}".strip()
            if text:  # Only include documents with text
                documents.append({
                    'newid': newid,
                    'title': title,
                    'text': text
                })
        except:
            continue

    return documents


def generate_k_shingles(text: str, k: int) -> List[str]:
    if not text or len(text) < k:
        return []
    text = ' '.join(text.lower().split())
    # Use set directly, no need to sort for hash-based operations
    return list(set(text[i:i + k] for i in range(len(text) - k + 1)))


def generate_minhash_signature(shingles_list: List[str], num_hashes: int, hash_params: List[Tuple[int, int]]) -> List[
    int]:
    if not shingles_list:
        return [2147483647] * num_hashes

    prime: int = 2147483647
    signature: List[int] = [prime] * num_hashes

    # Pre-compute shingle hashes
    shingle_hashes = [int(hashlib.md5(shg.encode()).hexdigest(), 16) % prime for shg in shingles_list]

    for i, (a, b) in enumerate(hash_params):
        min_hash = min((a * sh + b) % prime for sh in shingle_hashes)
        signature[i] = int(min_hash)

    return signature


def generate_lsh_bands(signature: List[int], bands_len: int, rows_per_band: int,
                       band_hash_params: List[List[Tuple[int, int]]]) -> List[Tuple[int, str]]:
    bands: List[Tuple[int, str]] = []
    prime: int = 2147483647

    for band_idx in range(bands_len):
        start_idx: int = band_idx * rows_per_band
        end_idx: int = start_idx + rows_per_band
        band_values: Tuple[int, ...] = tuple(signature[start_idx:end_idx])

        band_str: str = str(band_values)
        band_int_hash: int = int(hashlib.md5(band_str.encode()).hexdigest(), 16) % prime

        band_hashes: List[int] = [
            (a * band_int_hash + b) % prime
            for a, b in band_hash_params[band_idx]
        ]

        combined_hash: str = hashlib.md5(str(tuple(band_hashes)).encode()).hexdigest()
        bands.append((band_idx, combined_hash))

    return bands


def process_partition(documents_iter: Iterator[Dict[str, Any]], k: int, num_hashes: int,
                      bands_len: int, rows_per_band: int, hash_params: List[Tuple[int, int]],
                      band_hash_params: List[List[Tuple[int, int]]]) -> Iterator[
    Tuple[str, str, List[str], List[int], List[Tuple[int, str]]]]:
    for document in documents_iter:
        if document['text']:
            shingles_list: List[str] = generate_k_shingles(document['text'], k)
            if shingles_list:
                signature: List[int] = generate_minhash_signature(shingles_list, num_hashes, hash_params)
                bands: List[Tuple[int, str]] = generate_lsh_bands(signature, bands_len, rows_per_band, band_hash_params)
                yield (document['newid'], document['title'], shingles_list, signature, bands)


if __name__ == "__main__":
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

    K_SHINGLE_SIZE = args.k
    NUM_HASHES = args.H
    NUM_BANDS = args.b

    if args.r is not None:
        ROWS_PER_BAND = args.r
        if NUM_HASHES != NUM_BANDS * ROWS_PER_BAND:
            print(f"Warning: H ({NUM_HASHES}) != b ({NUM_BANDS}) * r ({ROWS_PER_BAND})")
    else:
        ROWS_PER_BAND = NUM_HASHES // NUM_BANDS
        if NUM_HASHES % NUM_BANDS != 0:
            print(f"Warning: H ({NUM_HASHES}) is not divisible by b ({NUM_BANDS})")

    NUM_BAND_HASHES = args.band_hashes

    print("=" * 80)
    print("LSH Similar Documents Pipeline - Configuration")
    print("=" * 80)
    print(f"k (k-shingle size): {K_SHINGLE_SIZE}")
    print(f"H (number of MinHash functions): {NUM_HASHES}")
    print(f"b (number of bands): {NUM_BANDS}")
    print(f"r (rows per band): {ROWS_PER_BAND}")
    print(f"Number of hash functions per band: {NUM_BAND_HASHES}")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName("Reuters Similar Documents") \
        .master("spark://192.168.0.2:7077") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.rpc.message.maxSize", "256") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Pre-generate hash parameters (broadcast once)
    prime = 2147483647
    random.seed(42)  # For reproducibility
    hash_params = [(random.randint(1, prime - 1), random.randint(0, prime - 1)) for _ in range(NUM_HASHES)]

    band_hash_params = []
    for band_idx in range(NUM_BANDS):
        random.seed(band_idx * 1000)
        band_hash_params.append(
            [(random.randint(1, prime - 1), random.randint(0, prime - 1)) for _ in range(NUM_BAND_HASHES)])

    print("\nLoading Reuters files from HDFS...")

    # Read all files at once using wildcard
    file_paths = [f"{DATA_DIR}/reut2-{i:03d}.sgm" for i in range(22)]

    all_documents = []
    for file_path in file_paths:
        try:
            file_rdd = sc.textFile(file_path)
            content = "\n".join(file_rdd.collect())
            docs = parse_reuters_xml(content)
            all_documents.extend(docs)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            continue

    print(f"\nTotal documents loaded: {len(all_documents)}")

    if not all_documents:
        print("No documents loaded. Exiting.")
        spark.stop()
        exit(1)

    # Broadcast hash parameters
    hash_params_bc = sc.broadcast(hash_params)
    band_hash_params_bc = sc.broadcast(band_hash_params)

    # Process documents in parallel
    docs_rdd = sc.parallelize(all_documents, numSlices=8)


    def process_with_params(docs_iter):
        return process_partition(
            docs_iter, K_SHINGLE_SIZE, NUM_HASHES, NUM_BANDS,
            ROWS_PER_BAND, hash_params_bc.value, band_hash_params_bc.value
        )


    processed_rdd = docs_rdd.mapPartitions(process_with_params).cache()

    # Get count without collecting all data
    num_processed = processed_rdd.count()

    if num_processed == 0:
        print("No documents with shingles found. Exiting.")
        spark.stop()
        exit(1)

    print(f"\nProcessing {num_processed} documents...")

    # === OPTIMIZATION 1: Use RDD operations for set representation ===
    print("\n" + "=" * 80)
    print("Generating Set Representation (MxN matrix)...")
    print("=" * 80)

    # Create RDD directly from processed_rdd without collecting
    from pyspark.sql import Row

    shingle_doc_rdd = processed_rdd.flatMap(
        lambda x: [Row(shingle=shingle, doc_id=x[0], value='1') for shingle in x[2]]
    )

    set_rep_df = spark.createDataFrame(shingle_doc_rdd)

    set_rep_path = f"{OUTPUT_DIR}/set_representation"
    print(f"Saving set representation to {set_rep_path}...")
    set_rep_df.repartition(4).write.mode("overwrite").csv(set_rep_path, header=True, sep=",")
    print(f"Set representation saved: {set_rep_df.count()} entries")

    # === OPTIMIZATION 2: Use RDD operations for signatures ===
    print("\n" + "=" * 80)
    print("Generating MinHash Signatures (HxN matrix)...")
    print("=" * 80)

    # Create RDD directly from processed_rdd without collecting
    signature_rdd = processed_rdd.flatMap(
        lambda x: [Row(hash_function_index=str(i), doc_id=x[0], signature_value=str(sig_val))
                   for i, sig_val in enumerate(x[3])]
    )

    sig_df = spark.createDataFrame(signature_rdd)

    sig_path = f"{OUTPUT_DIR}/minhash_signatures"
    print(f"Saving MinHash signatures to {sig_path}...")
    sig_df.repartition(4).write.mode("overwrite").csv(sig_path, header=True, sep=",")
    print(f"MinHash signatures saved: {sig_df.count()} entries")

    # === OPTIMIZATION 3: Use RDD operations for candidate pairs ===
    print("\n" + "=" * 80)
    print("Finding Candidate Pairs...")
    print("=" * 80)

    # Extract bands and create (band_id, band_hash, doc_id) tuples
    bands_rdd = processed_rdd.flatMap(
        lambda x: [((band_id, band_hash), x[0]) for band_id, band_hash in x[4]]
    )

    # Group by (band_id, band_hash) to find documents in same bucket
    buckets_rdd = bands_rdd.groupByKey().mapValues(list)


    # Generate pairs from buckets and count band co-occurrences
    def generate_pairs(bucket_docs):
        pairs = []
        docs = list(bucket_docs)
        if len(docs) > 1:
            for i in range(len(docs)):
                for j in range(i + 1, len(docs)):
                    pair = tuple(sorted([docs[i], docs[j]]))
                    pairs.append((pair, 1))
        return pairs


    pair_counts_rdd = buckets_rdd.flatMap(lambda x: generate_pairs(x[1])) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] > 1)  # Only pairs in 2+ bands

    # Convert to DataFrame directly without collecting
    candidate_pairs_rdd = pair_counts_rdd.map(
        lambda x: Row(doc_id_1=x[0][0], doc_id_2=x[0][1], num_bands=str(x[1]))
    )

    candidate_df = spark.createDataFrame(candidate_pairs_rdd)

    num_candidates = candidate_df.count()

    if num_candidates == 0:
        print("No candidate pairs found (pairs in more than one band).")
    else:
        candidate_df = candidate_df.orderBy(col("num_bands").cast(IntegerType()).desc())

        candidate_path = f"{OUTPUT_DIR}/candidate_pairs"
        print(f"Saving candidate pairs to {candidate_path}...")
        candidate_df.repartition(1).write.mode("overwrite").csv(candidate_path, header=True, sep=",")
        print(f"Candidate pairs saved: {num_candidates} pairs")
        candidate_df.show(20, truncate=False)

    print("\n" + "=" * 80)
    print("Pipeline completed!")
    print("=" * 80)
    print(f"\nOutput files saved to HDFS:")
    print(f"  1. Set Representation: {OUTPUT_DIR}/set_representation")
    print(f"  2. MinHash Signatures: {OUTPUT_DIR}/minhash_signatures")
    print(f"  3. Candidate Pairs: {OUTPUT_DIR}/candidate_pairs")
    print("=" * 80)

    # Cleanup
    hash_params_bc.unpersist()
    band_hash_params_bc.unpersist()
    processed_rdd.unpersist()

    spark.stop()