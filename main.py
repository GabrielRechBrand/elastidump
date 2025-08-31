# main.py
from connection import get_es_client
from exporter import export_to_file
from colorama import Fore, Style, init

init(autoreset=True)  # Para reset automático das cores

def main():
    es = get_es_client()

    # Pegar índices "normais"
    all_indices = es.cat.indices(format="json")
    index_names = [i['index'] for i in all_indices if not i['index'].startswith('.')]

    # Menu numérico para escolher índice
    print(Fore.CYAN + "\n📂 Available indices:")
    for idx, name in enumerate(index_names, 1):
        print(Fore.YELLOW + f"[{idx}] {name}")

    while True:
        try:
            choice = int(input(Fore.CYAN + "\nChoose the index by typing its number: "))
            if 1 <= choice <= len(index_names):
                index_name = index_names[choice - 1]
                print(Fore.GREEN + f"✅ Selected index: {index_name}")
                break
            else:
                print(Fore.RED + "❌ Invalid number, try again.")
        except ValueError:
            print(Fore.RED + "❌ Please enter a valid number.")

    # Nome do arquivo
    backup_file = input(Fore.CYAN + f"Type backup file name (default: backup_{index_name}.jsonl): ").strip()
    if not backup_file:
        backup_file = f"backup_{index_name}.jsonl"
    print(Fore.GREEN + f"📄 Backup file: {backup_file}")

    # Pegando os campos dinamicamente do mapping
    mapping = es.indices.get_mapping(index=index_name)
    doc_fields = list(mapping[index_name]['mappings']['properties'].keys())

    print(Fore.CYAN + "\n📝 Available fields to export:")
    for idx, field in enumerate(doc_fields, 1):
        print(Fore.YELLOW + f"[{idx}] {field}")
    print(Fore.CYAN + "Type the numbers separated by commas (e.g., 1,3,4) or leave empty for all:")

    while True:
        fields_input = input().strip()
        if not fields_input:
            fields_to_export = doc_fields
            break
        try:
            selected_numbers = [int(f.strip()) for f in fields_input.split(',')]
            if all(1 <= n <= len(doc_fields) for n in selected_numbers):
                fields_to_export = [doc_fields[n - 1] for n in selected_numbers]
                break
            else:
                print(Fore.RED + "❌ Some numbers are invalid, try again.")
        except ValueError:
            print(Fore.RED + "❌ Enter only numbers separated by commas.")

    # Pergunta se é teste ou real
    while True:
        test_input = input(Fore.CYAN + "\n⚡ Import mode: type 't' for test (10 docs) or 'r' for real: ").lower()
        if test_input in ['t', 'r']:
            test_mode = (test_input == 't')
            break
        else:
            print(Fore.RED + "❌ Invalid input. Type 't' for test or 'r' for real.")

    # Executa exportação
    export_to_file(es, index_name, backup_file, fields_to_export, test_mode)

if __name__ == "__main__":
    main()
