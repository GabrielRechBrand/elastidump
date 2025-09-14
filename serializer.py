# serializer.py
import json
import csv
from pathlib import Path

def ensure_extension(file_path, file_format):
    ext_map = {"jsonl": ".jsonl", "json": ".json", "csv": ".csv"}
    ext = ext_map.get(file_format.lower(), ".jsonl")
    if not str(file_path).lower().endswith(ext):
        file_path = f"{file_path}{ext}"
    return file_path

def save_to_file(data, file_path, file_format="jsonl", fields=None):
    file_path = ensure_extension(file_path, file_format)
    file_path = Path(file_path)

    if file_path.exists():
        file_path.unlink()  # remove antigo

    if file_format.lower() == "jsonl":
        with file_path.open("w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
    elif file_format.lower() == "json":
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    elif file_format.lower() == "csv":
        with file_path.open("w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for item in data:
                writer.writerow(item)
    else:
        # default para jsonl
        with file_path.open("w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    return file_path
