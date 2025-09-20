import json
import csv
from pathlib import Path
from fetcher import fetch_documents


def ensure_extension(file_path, file_format):
    ext_map = {"jsonl": ".jsonl", "json": ".json", "csv": ".csv"}
    ext = ext_map.get(file_format.lower(), ".jsonl")
    if not str(file_path).lower().endswith(ext):
        file_path = f"{file_path}{ext}"
    return file_path


def export_to_file(es, index_name, backup_file, fields_to_export, file_format="jsonl", test_mode=False, include_id=False):
    batch_size = 10 if test_mode else 5000

    # Ajusta campos de saída
    fields_out = list(fields_to_export)
    if include_id and 'id' not in fields_out:
        fields_out.append('id')

    backup_file = ensure_extension(backup_file, file_format)
    backup_file = Path(backup_file)

    if backup_file.exists():
        backup_file.unlink()

    # Escrita em streaming
    with backup_file.open("w", encoding="utf-8", newline='') as f:
        writer = None
        if file_format.lower() == "csv":
            writer = csv.DictWriter(f, fieldnames=fields_out)
            writer.writeheader()

        first = True
        if file_format.lower() == "json":
            f.write("[\n")

        for source, doc_id, processed, total_docs in fetch_documents(
            es, index_name, scroll_time="2m", batch_size=batch_size,
            test_mode=test_mode, fields_to_export=fields_to_export
        ):
            export_data = {field: source.get(field, None) for field in fields_to_export}
            if include_id:
                export_data['id'] = doc_id

            if file_format.lower() == "jsonl":
                f.write(json.dumps(export_data, ensure_ascii=False) + "\n")
            elif file_format.lower() == "json":
                if not first:
                    f.write(",\n")
                f.write(json.dumps(export_data, ensure_ascii=False, indent=2))
                first = False
            elif file_format.lower() == "csv":
                writer.writerow(export_data)

        if file_format.lower() == "json":
            f.write("\n]\n")

    print(f"\n✅ Export finished! File saved to {backup_file}")
