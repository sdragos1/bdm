import argparse
import hashlib
import math
import random
import re
import xml.etree.ElementTree as Element
from collections import Counter
from dataclasses import dataclass
from typing import List, Dict, Tuple, Iterator

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, Row

HDFS_NAMENODE: str = "hdfs://192.168.0.12:9000"
DATA_DIR: str = f"{HDFS_NAMENODE}/input/similar_documents"
OUTPUT_DIR: str = f"{HDFS_NAMENODE}/output/similar_documents"


@dataclass
class EffectivenessMetrics:
    true_positives: int
    false_positives: int
    false_negatives: int
    true_negatives: int
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    documents_with_no_topics: int = 0

    def print_metrics(self):
        print("\n" + "=" * 80)
        print("EFFECTIVENESS METRICS")
        print("=" * 80)
        print(f"True Positives (TP):   {self.true_positives:,}")
        print(f"False Positives (FP):  {self.false_positives:,}")
        print(f"False Negatives (FN):  {self.false_negatives:,}")
        print(f"True Negatives (TN):   {self.true_negatives:,}")
        print("-" * 80)
        print(f"Accuracy:              {self.accuracy:.4f} ({self.accuracy * 100:.2f}%)")
        print(f"Precision:             {self.precision:.4f} ({self.precision * 100:.2f}%)")
        print(f"Recall (Sensitivity):  {self.recall:.4f} ({self.recall * 100:.2f}%)")
        print(f"F1 Score:              {self.f1_score:.4f}")
        print("-" * 80)
        print(f"Document with no topics:   {self.documents_with_no_topics:,}")
        print("=" * 80)


@dataclass
class Document:
    newid: str
    body: str
    topics: List[str]


def parse_reuters_xml(xml_content: str, test_mode: bool) -> List[Document]:
    documents: List[Document] = []
    reuters_pattern: str = r'<REUTERS[^>]*>.*?</REUTERS>'
    doc_matches: List[str] = re.findall(reuters_pattern, xml_content, re.DOTALL)

    for document in doc_matches:
        try:
            document = re.sub(r'&#\d+;', ' ', document)
            root = Element.fromstring(document)

            test_split = root.get('LEWISSPLIT', '') == 'TEST'
            if test_mode and not test_split:
                continue

            newid: str = root.get('NEWID', '')

            body: str = ''

            text_elem = root.find('TEXT')
            if text_elem is not None:
                body_elem = text_elem.find('BODY')
                if body_elem is not None and body_elem.text:
                    body = body_elem.text.strip()

            topics: list[str] = [d.text for d in root.findall('./TOPICS/D') if d.text]

            doc = Document(newid=newid, body=body, topics=topics)
            documents.append(doc)
        except:
            continue

    return documents


def generate_k_shingles(text: str, k: int) -> List[str]:
    if not text:
        return []

    text = text.lower()

    text = re.sub(r'\breuter\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\d{1,2}-[A-Z]{3}-\d{2,4}', '', text)

    stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'}

    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [t for t in text.split() if t and t not in stopwords]

    if len(tokens) < k:
        return []

    shingles = []
    for i in range(len(tokens) - k + 1):
        shingle = " ".join(tokens[i:i + k])
        shingles.append(shingle)

    return list(set(shingles))


def compute_idf(all_documents: List[Document], k: int) -> Dict[str, float]:
    """Compute IDF for all shingles"""
    doc_count = len(all_documents)
    shingle_doc_freq = Counter()

    for doc in all_documents:
        if doc.body:
            shingles = set(generate_k_shingles(doc.body, k))
            for shingle in shingles:
                shingle_doc_freq[shingle] += 1

    idf = {}
    for shingle, freq in shingle_doc_freq.items():
        idf[shingle] = math.log(doc_count / freq)

    return idf


def generate_minhash_signature_weighted(shingles_list: List[str],
                                        num_hashes: int,
                                        hash_params: List[Tuple[int, int]],
                                        idf_scores: Dict[str, float]) -> List[int]:
    if not shingles_list:
        return [2147483647] * num_hashes

    prime = 2147483647
    signature = [prime] * num_hashes

    weighted_shingles = []
    for shingle in shingles_list:
        weight = int(idf_scores.get(shingle, 1.0) * 3)
        weighted_shingles.extend([shingle] * max(1, weight))

    shingle_hashes = [int(hashlib.md5(shg.encode()).hexdigest(), 16) % prime
                      for shg in weighted_shingles]

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


def process_partition(documents_iter: Iterator[Document], k: int, num_hashes: int,
                      bands_len: int, rows_per_band: int, hash_params: List[Tuple[int, int]],
                      band_hash_params: List[List[Tuple[int, int]]], idf_scores: Dict[str, float]) -> Iterator[
    Tuple[str, str, List[str], List[int], List[Tuple[int, str]]]]:
    for document in documents_iter:
        if document.body:
            shingles_list: List[str] = generate_k_shingles(document.body, k)
            if shingles_list:
                signature: List[int] = generate_minhash_signature_weighted(shingles_list, num_hashes, hash_params,
                                                                           idf_scores)
                bands: List[Tuple[int, str]] = generate_lsh_bands(signature, bands_len, rows_per_band, band_hash_params)
                yield document.newid, document.body, shingles_list, signature, bands


def compute_effectiveness_metrics(candidate_pairs_df: DataFrame, all_docs: List[Document]) -> EffectivenessMetrics:
    doc_topics: Dict[str, set] = {doc.newid: set(doc.topics) for doc in all_docs if doc.topics}
    doc_ids = list(doc_topics.keys())

    candidate_set = set()
    for row in candidate_pairs_df.collect():
        candidate_set.add((row.doc_id_1, row.doc_id_2))
        candidate_set.add((row.doc_id_2, row.doc_id_1))

    tp = fp = fn = tn = 0

    for i in range(len(doc_ids)):
        for j in range(i + 1, len(doc_ids)):
            d1, d2 = doc_ids[i], doc_ids[j]
            actual_similar = len(doc_topics[d1].intersection(doc_topics[d2])) > 0
            predicted_similar = (d1, d2) in candidate_set

            if predicted_similar and actual_similar:
                tp += 1
            elif predicted_similar and not actual_similar:
                fp += 1
            elif not predicted_similar and actual_similar:
                fn += 1
            else:
                tn += 1

    accuracy = (tp + tn) / (tp + tn + fp + fn)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    return EffectivenessMetrics(
        true_positives=tp,
        false_positives=fp,
        false_negatives=fn,
        true_negatives=tn,
        accuracy=accuracy,
        precision=precision,
        recall=recall,
        f1_score=f1_score,
        documents_with_no_topics=len(all_docs) - len(doc_topics)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='LSH Similar Documents Pipeline')
    parser.add_argument('--k', type=int, default=3,
                        help='k-shingle size (default: 3)')
    parser.add_argument('--H', type=int, default=100,
                        help='Number of MinHash functions (default: 100)')
    parser.add_argument('--b', type=int, default=50,
                        help='Number of bands (default: 50)')
    parser.add_argument('--r', type=int, default=None,
                        help='Number of rows per band (default: H/b)')
    parser.add_argument('--band-hashes', type=int, default=2,
                        help='Number of hash functions for hashing each band (default: 2)')
    parser.add_argument('--min-bands', type=int, default=1,
                        help='Minimum number of band matches to consider pair as candidate (default: 1)')
    parser.add_argument('--test', action='store_true', default=False,
                        help='Run the pipeline on the test split (default: False)')

    args = parser.parse_args()

    K_SHINGLE_SIZE = args.k
    NUM_HASHES = args.H
    NUM_BANDS = args.b
    MIN_BAND_MATCHES = args.min_bands

    if args.r is None:
        ROWS_PER_BAND = NUM_HASHES // NUM_BANDS
        if NUM_HASHES % NUM_BANDS != 0:
            print(f"Warning: H ({NUM_HASHES}) not divisible by b ({NUM_BANDS}), using r={ROWS_PER_BAND}")
    else:
        ROWS_PER_BAND = args.r
        NUM_BANDS = NUM_HASHES // ROWS_PER_BAND
        if NUM_HASHES % ROWS_PER_BAND != 0:
            raise ValueError("H must be multiple of r")

    NUM_BAND_HASHES = args.band_hashes

    print("=" * 80)
    print("LSH Similar Documents Pipeline - Configuration")
    print("=" * 80)
    print(f"k (k-shingle size): {K_SHINGLE_SIZE}")
    print(f"H (number of MinHash functions): {NUM_HASHES}")
    print(f"b (number of bands): {NUM_BANDS}")
    print(f"r (rows per band): {ROWS_PER_BAND}")
    print(f"Number of hash functions per band: {NUM_BAND_HASHES}")
    print(f"Minimum band matches for candidates: {MIN_BAND_MATCHES}")
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

    prime = 2147483647
    random.seed(42)
    hash_params = [(random.randint(1, prime - 1), random.randint(0, prime - 1)) for _ in range(NUM_HASHES)]

    band_hash_params = []
    for band_idx in range(NUM_BANDS):
        random.seed(band_idx * 1000)
        band_hash_params.append(
            [(random.randint(1, prime - 1), random.randint(0, prime - 1)) for _ in range(NUM_BAND_HASHES)])

    print("\nLoading Reuters files from HDFS...")

    file_paths = [f"{DATA_DIR}/reut2-{i:03d}.sgm" for i in range(22)]

    all_documents: List[Document] = []
    for file_path in file_paths:
        try:
            file_rdd = sc.textFile(file_path)
            content = "\n".join(file_rdd.collect())
            docs = parse_reuters_xml(content, args.test)
            all_documents.extend(docs)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            continue

    print(f"\nTotal documents loaded: {len(all_documents)}")

    if not all_documents:
        print("No documents loaded. Exiting.")
        spark.stop()
        exit(1)

    hash_params_bc = sc.broadcast(hash_params)
    band_hash_params_bc = sc.broadcast(band_hash_params)

    docs_rdd = sc.parallelize(all_documents, numSlices=8)

    print("\nComputing IDF scores...")
    idf_scores = compute_idf(all_documents, K_SHINGLE_SIZE)
    idf_scores_bc = sc.broadcast(idf_scores)


    def process_with_params(docs_iter):
        return process_partition(
            docs_iter, K_SHINGLE_SIZE, NUM_HASHES, NUM_BANDS,
            ROWS_PER_BAND, hash_params_bc.value, band_hash_params_bc.value, idf_scores_bc.value
        )


    processed_rdd = docs_rdd.mapPartitions(process_with_params).cache()

    num_processed = processed_rdd.count()

    if num_processed == 0:
        print("No documents with shingles found. Exiting.")
        spark.stop()
        exit(1)

    print(f"\nProcessing {num_processed} documents... (only those with body and shingles)")

    print("\n" + "=" * 80)
    print("Generating Set Representation (MxN matrix: Shingles x Documents)...")
    print("=" * 80)

    shingle_doc_rdd = processed_rdd.flatMap(
        lambda x: [Row(shingle=shingle, doc_id=x[0], value='1') for shingle in x[2]]
    )
    set_rep_df = spark.createDataFrame(shingle_doc_rdd)

    set_rep_path = f"{OUTPUT_DIR}/set_representation"
    print(f"Saving set representation to {set_rep_path}...")
    set_rep_df.repartition(4).write.mode("overwrite").csv(set_rep_path, header=True, sep=",")

    print("\n" + "=" * 80)
    print("Generating MinHash Signatures (HxN matrix: Hash Functions x Documents)...")
    print("=" * 80)

    signature_rdd = processed_rdd.flatMap(
        lambda x: [Row(hash_function_index=str(i), doc_id=x[0], signature_value=str(sig_val))
                   for i, sig_val in enumerate(x[3])]
    )
    sig_df = spark.createDataFrame(signature_rdd)

    sig_path = f"{OUTPUT_DIR}/minhash_signatures"
    sig_df.repartition(4).write.mode("overwrite").csv(sig_path, header=True, sep=",")
    print(f"MinHash signatures saved: {sig_df.count()} entries")

    print("\n" + "=" * 80)
    print("Finding Candidate Pairs...")
    print("=" * 80)

    bands_rdd = processed_rdd.flatMap(
        lambda x: [((band_id, band_hash), x[0]) for band_id, band_hash in x[4]]
    )

    buckets_rdd = bands_rdd.groupByKey().mapValues(list)


    def generate_pairs(bucket_docs):
        docs = list(bucket_docs)
        if len(docs) < 2:
            return []
        pairs = []
        for i in range(len(docs)):
            for j in range(i + 1, len(docs)):
                pair = tuple(sorted([docs[i], docs[j]]))
                pairs.append((pair, 1))
        return pairs


    pair_counts_rdd = buckets_rdd.flatMap(lambda x: generate_pairs(x[1])) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] >= MIN_BAND_MATCHES)

    candidate_pairs_rdd = pair_counts_rdd.map(
        lambda x: Row(doc_id_1=x[0][0], doc_id_2=x[0][1], num_bands_matched=x[1])
    )

    candidate_df = spark.createDataFrame(candidate_pairs_rdd)

    num_candidates = candidate_df.count()

    if num_candidates == 0:
        print(f"No candidate pairs found (pairs matching in >= {MIN_BAND_MATCHES} bands).")
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        empty_schema = StructType([
            StructField("doc_id_1", StringType(), True),
            StructField("doc_id_2", StringType(), True),
            StructField("num_bands_matched", IntegerType(), True)
        ])
        candidate_df = spark.createDataFrame([], empty_schema)
    else:
        candidate_df = candidate_df.orderBy(col("num_bands_matched").cast(IntegerType()).desc())

        candidate_path = f"{OUTPUT_DIR}/candidate_pairs"
        print(f"Saving candidate pairs to {candidate_path}...")
        candidate_df.coalesce(1).write.mode("overwrite").csv(candidate_path, header=True, sep=",")
        print(f"Candidate pairs saved: {num_candidates} pairs")
        candidate_df.show(20, truncate=False)

    metrics = compute_effectiveness_metrics(candidate_df, all_documents)
    metrics.print_metrics()

    print("\n" + "=" * 80)
    print("Pipeline completed!")
    print("=" * 80)

    hash_params_bc.unpersist()
    band_hash_params_bc.unpersist()
    idf_scores_bc.unpersist()
    processed_rdd.unpersist()

    spark.stop()
