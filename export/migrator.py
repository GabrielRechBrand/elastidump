import json
import boto3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==== CONFIGS ====
JSONL_FILE = ""
INDEX_NAME = ""
BUCKET_NAME = ""
FOLDER_NAME = INDEX_NAME

# ==== CLIENT ====
s3 = boto3.client("s3")

def upload_doc(doc):
    doc_id = doc.get("id")
    if not doc_id:
        raise ValueError("Documento sem campo 'id'")
    key = f"{FOLDER_NAME}/{doc_id}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(doc, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )
    return doc_id

def export_jsonl_to_s3():
    count = 0
    with open(JSONL_FILE, "r", encoding="utf-8") as f, ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for line in f:
            if not line.strip():
                continue
            doc = json.loads(line)
            futures.append(executor.submit(upload_doc, doc))

        for future in as_completed(futures):
            count += 1
            if count % 100 == 0:
                print(f"Exportados {count} documentos...")

    print(f"✅ Exportação concluída: {count} documentos salvos em s3://{BUCKET_NAME}/{FOLDER_NAME}/")

if __name__ == "__main__":
    export_jsonl_to_s3()
