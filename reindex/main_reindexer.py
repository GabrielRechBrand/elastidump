from config.connection import get_es_client
from reindex.reindexer import reindex_index
from colorama import Fore, init

init(autoreset=True)

def main():
    es = get_es_client()

    all_indices = es.cat.indices(format="json")
    index_names = [i['index'] for i in all_indices if not i['index'].startswith('.')]

    print(Fore.CYAN + "\n📂 Available indices:")
    for idx, name in enumerate(index_names, 1):
        print(Fore.YELLOW + f"[{idx}] {name}")

    while True:
        try:
            choice = int(input(Fore.CYAN + "\nChoose source index: "))
            if 1 <= choice <= len(index_names):
                source_index = index_names[choice - 1]
                print(Fore.GREEN + f"✅ Selected source index: {source_index}")
                break
            else:
                print(Fore.RED + "❌ Invalid number.")
        except ValueError:
            print(Fore.RED + "❌ Enter a valid number.")

    target_index = input(Fore.CYAN + "\nType target index name: ").strip()
    if not target_index:
        target_index = f"{source_index}_reindexed"
    print(Fore.GREEN + f"📄 Target index: {target_index}")

    mapping = es.indices.get_mapping(index=source_index)
    doc_fields = list(mapping[source_index]['mappings']['properties'].keys())

    print(Fore.CYAN + "\n📝 Available fields:")
    for idx, field in enumerate(doc_fields, 1):
        print(Fore.YELLOW + f"[{idx}] {field}")
    print(Fore.CYAN + "Type numbers separated by commas (or leave empty for all):")

    while True:
        fields_input = input().strip()
        if not fields_input:
            fields_to_use = doc_fields
            break
        try:
            selected = [int(f.strip()) for f in fields_input.split(',')]
            if all(1 <= n <= len(doc_fields) for n in selected):
                fields_to_use = [doc_fields[n - 1] for n in selected]
                break
            else:
                print(Fore.RED + "❌ Invalid selection.")
        except ValueError:
            print(Fore.RED + "❌ Only numbers separated by commas.")

    while True:
        test_input = input(Fore.CYAN + "\n⚡ Mode: 't' for test (10 docs) or 'r' for real: ").lower()
        if test_input in ['t', 'r']:
            test_mode = (test_input == 't')
            break
        else:
            print(Fore.RED + "❌ Invalid input.")

    reindex_index(es, source_index, target_index, fields_to_use, test_mode=test_mode)

if __name__ == "__main__":
    main()
