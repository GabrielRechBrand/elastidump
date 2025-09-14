# exporter.py
from fetcher import fetch_documents
from serializer import save_to_file

def export_to_file(es, index_name, backup_file, fields_to_export, file_format="jsonl", test_mode=False):
    batch_size = 10 if test_mode else 1000  # pode vir de config

    all_data = []
    for doc in fetch_documents(es, index_name, scroll_time="2m", batch_size=batch_size, test_mode=test_mode):
        export_data = {field: doc.get(field, None) for field in fields_to_export}
        all_data.append(export_data)

    save_to_file(all_data, backup_file, file_format=file_format, fields=fields_to_export)
