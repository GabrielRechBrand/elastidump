# exporter.py
import json
import os
from config import SCROLL_TIME, BATCH_SIZE

def export_to_file(es, index_name, backup_file, fields_to_export, test_mode=False):
    if os.path.exists(backup_file):
        os.remove(backup_file)

    res = es.search(
        index=index_name,
        scroll=SCROLL_TIME,
        query={"match_all": {}},
        size=BATCH_SIZE
    )

    scroll_id = res['_scroll_id']
    total_docs = res['hits']['total']['value']
    print(f"Total documents: {total_docs}")

    processed = 0
    try:
        while len(res['hits']['hits']):
            with open(backup_file, 'a', encoding='utf-8') as f:
                for doc in res['hits']['hits']:
                    source = doc['_source']
                    process_number = source.get('processNumber', doc['_id'])

                    export_data = {'processNumber': process_number}
                    for field in fields_to_export:
                        export_data[field] = source.get(field)

                    f.write(json.dumps(export_data, ensure_ascii=False) + '\n')
                    processed += 1

                    if test_mode and processed >= 10:
                        print("⚠️ Test export limit reached (10 documents).")
                        return

            print(f"Documents processed: {processed}")
            res = es.scroll(
                scroll_id=scroll_id,
                scroll=SCROLL_TIME
            )

    except KeyboardInterrupt:
        print("\n⚠️ Export interrupted by the user.")
        print(f"👉 Documents saved so far: {processed}")

    finally:
        try:
            es.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass
        print("🔚 Export finished.")
