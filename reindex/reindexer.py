from elasticsearch import Elasticsearch, helpers
from colorama import Fore

def reindex_index(es, source_index, target_index, fields, test_mode=False, batch_size=1000):
    print(Fore.CYAN + f"\n🚀 Starting reindex from '{source_index}' to '{target_index}'")

    if not es.indices.exists(index=target_index):
        es.indices.create(index=target_index)
        print(Fore.GREEN + f"✅ Created new index: {target_index}")

    query = {
        "query": {"match_all": {}}
    }

    docs = helpers.scan(
        client=es,
        query=query,
        index=source_index,
        size=batch_size,
        _source=fields
    )

    actions = []
    count = 0

    for doc in docs:
        new_doc = {
            "_op_type": "index",
            "_index": target_index,
            "_id": doc["_id"],
            "_source": doc["_source"]
        }
        actions.append(new_doc)
        count += 1

        if test_mode and count >= 10:
            break

        if not test_mode and len(actions) >= batch_size:
            helpers.bulk(es, actions)
            actions = []

    if actions:
        helpers.bulk(es, actions)

    print(Fore.GREEN + f"✅ Reindex completed! {count} documents processed.")
