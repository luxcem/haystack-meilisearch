import logging

from haystack.backends import BaseEngine, BaseSearchBackend, BaseSearchQuery
from haystack.models import SearchResult
from meilisearch import Client


class MeiliSearchBackend(BaseSearchBackend):
    def __init__(self, connection_alias, **connection_options):
        super().__init__(connection_alias, **connection_options)
        if "URL" not in connection_options:
            raise Exception("URL not set")
        self.url = connection_options["URL"]

        if "KEY" not in connection_options:
            raise Exception("KEY not set")
        self.key = connection_options["KEY"]
        self.client = Client(self.url, self.key)
        self.log = logging.getLogger("haystack")

    def clear(self, models=None, commit=True):
        index = self.client.index("haystack")
        index.delete_all_documents()

    def update(self, index, iterable, commit=True):
        documents = [index.full_prepare(obj) for obj in iterable]
        for document in documents:
            document["id"] = document["id"].replace(".", "_")
        meili_index = self.client.index("haystack")
        meili_index.add_documents(documents)

    def search(self, query_string, result_class=None, **kwargs):
        meili_index = self.client.index("haystack")
        raw_results = meili_index.search(query_string)

        if result_class is None:
            result_class = SearchResult

        results = []
        for match in raw_results["hits"]:
            app_label, model_name, pk = match["id"].split("_")
            results.append(result_class(app_label=app_label, model_name=model_name, pk=pk, score=0))

        return {
            "results": results,
            "hits": len(results)
        }

class MeiliSearchQuery(BaseSearchQuery):

    def build_query_fragment(self, field, filter_type, value):
        return str(value)




class MeiliSearchEngine(BaseEngine):
    backend = MeiliSearchBackend
    query = MeiliSearchQuery
