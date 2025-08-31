# exporter.py
import json
import os
from config import SCROLL_TIME, BATCH_SIZE
from colorama import Fore, Style

def export_to_file(es, index_name, backup_file, fields_to_export, test_mode=False):
    if os.path.exists(backup_file):
        os.remove(backup_file)

    batch_size = 10 if test_mode else BATCH_SIZE

    res = es.search(
        index=index_name,
        scroll=SCROLL_TIME,
        query={"match_all": {}},
        size=batch_size
    )

    scroll_id = res['_scroll_id']
    total_docs = res['hits']['total']['value'] if not test_mode else min(10, res['hits']['total']['value'])
    print(Fore.CYAN + f"\n📊 Total documents to export: {total_docs}")

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
                    # Progresso em tempo real
                    print(Fore.GREEN + f"\r📄 Exported {processed}/{total_docs} documents...", end='')

                    if test_mode and processed >= 10:
                        break

            if test_mode and processed >= 10:
                break

            res = es.scroll(scroll_id=scroll_id, scroll=SCROLL_TIME)

    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n⚠️ Export interrupted by user.")
        print(Fore.CYAN + f"👉 Documents saved so far: {processed}")

    finally:
        try:
            es.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass
        print(Fore.MAGENTA + "\n🔚 Export finished.")
