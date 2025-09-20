from colorama import Fore


def fetch_documents(es, index_name, scroll_time, batch_size, test_mode=False, max_test_docs=10, fields_to_export=None):
    """
    Generator para buscar documentos do Elasticsearch usando scroll,
    trazendo apenas os campos necessários (_source_includes).
    """
    res = es.search(
        index=index_name,
        scroll=scroll_time,
        query={"match_all": {}},
        size=batch_size,
        _source_includes=fields_to_export  # só traz os campos necessários
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
                yield doc.get('_source', {}), doc.get('_id'), processed, total_docs
                processed += 1

                # imprime a cada 1000 docs (em vez de cada doc)
                if processed % 1000 == 0 or processed == total_docs:
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
