# main.py
from connection import get_es_client
from exporter import export_to_file

def main():
    es = get_es_client()

    # Get "normal" indices
    all_indices = es.cat.indices(format="json")
    index_names = [i['index'] for i in all_indices if not i['index'].startswith('.')]

    # Numeric menu to select index
    print("Available indices:")
    for idx, name in enumerate(index_names, 1):
        print(f"[{idx}] {name}")

    while True:
        try:
            choice = int(input("Choose the index by typing the corresponding number: "))
            if 1 <= choice <= len(index_names):
                index_name = index_names[choice - 1]
                break
            else:
                print("❌ Invalid number, try again.")
        except ValueError:
            print("❌ Please enter a valid number.")

    # Backup file name
    backup_file = input(f"Enter backup file name (default: backup_{index_name}.jsonl): ").strip()
    if not backup_file:
        backup_file = f"backup_{index_name}.jsonl"

    # Possible fields
    possible_fields = ['additionalFields', 'embedding', 'embeddingText', 'summary']

    # Numeric menu to select fields
    print("Available fields to export:")
    for idx, field in enumerate(possible_fields, 1):
        print(f"[{idx}] {field}")
    print("Enter the numbers separated by comma (e.g., 1,3,4) or leave empty for all:")

    while True:
        fields_input = input().strip()
        if not fields_input:
            fields_to_export = possible_fields
            break
        try:
            selected_numbers = [int(f.strip()) for f in fields_input.split(',')]
            if all(1 <= n <= len(possible_fields) for n in selected_numbers):
                fields_to_export = [possible_fields[n - 1] for n in selected_numbers]
                break
            else:
                print("❌ Some numbers are invalid, try again.")
        except ValueError:
            print("❌ Please enter only numbers separated by comma.")

    # Ask if full export or test export
    while True:
        export_type = input("Do you want a full export or a test export (10 documents)? Enter 'full' or 'test': ").strip().lower()
        if export_type in ['full', 'test']:
            test_mode = (export_type == 'test')
            break
        else:
            print("❌ Invalid option, please type 'full' or 'test'.")

    # Execute export
    export_to_file(es, index_name, backup_file, fields_to_export, test_mode=test_mode)

if __name__ == "__main__":
    main()
