# fetcher.py
from colorama import Fore

def fetch_documents(es, index_name, scroll_time, batch_size, test_mode=False, max_test_docs=10):
    """
    Generator para buscar documentos do Elasticsearch usando scroll.
    """
    res = es.search(
        index=index_name,
        scroll=scroll_time,
        query={"match_all": {}},
        size=batch_size
    )

    scroll_id = res['_scroll_id']
    total_docs = res['hits']['total']['value']
    if test_mode:
        total_docs = min(max_test_docs, total_docs)
    print(Fore.CYAN + f"\n📊 Total documents to export: {total_docs}")

    processed = 0

    try:
        while res['hits']['hits']:
            for doc in res['hits']['hits']:
                yield doc['_source']
                processed += 1

                print(Fore.GREEN + f"\r📄 Exported {processed}/{total_docs} documents...", end='')

                if test_mode and processed >= max_test_docs:
                    return

            res = es.scroll(scroll_id=scroll_id, scroll=scroll_time)

    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n⚠️ Export interrupted by user.")
        print(Fore.CYAN + f"👉 Documents saved so far: {processed}")

    finally:
        try:
            es.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass
        print(Fore.MAGENTA + "\n🔚 Fetch finished.")
