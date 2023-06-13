import logging
from typing import Optional, Type, TYPE_CHECKING

from haystack.backends import BaseEngine, BaseSearchBackend, BaseSearchQuery
from haystack.models import SearchResult
from haystack import connections

from haystack.utils import get_model_ct, get_identifier
from meilisearch import Client
from django.db.models import Model
from collections.abc import Iterable
from haystack.indexes import SearchIndex


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

    def _index_name(self, model):
        """
        Return the name of the index for a given model by replacing the dot in
        the model's content type with an underscore.

        :param model: The `model` parameter is an instance of a Django model class. It is used to get
        the content type of the model using the `get_model_ct` function. The content type is then used
        to generate an index name by replacing the dot with an underscore
        :return: Returns a string that is index name for the given model
        """
        return get_model_ct(model).replace(".", "_")

    def clear(self, models: Optional[list[Type[Model]]] = None, commit: bool = True):
        """
        Clear indexes.

        :param models: Optional parameter that takes a list of Model objects. If
        provided, only the indexes associated with the specified models will be
        deleted. If not provided, all indexes in the Algolia account will be
        deleted
        :param commit: not used
        """
        indexes = self.client.get_indexes()["results"]
        if not models:
            for index in indexes:
                index.delete()
        else:
            for model in models:
                index_name = self._index_name(model)
                self.client.index(index_name).delete()

    def remove(self, obj_or_string: str | Model, commit: bool = True):
        """
        This function removes a document from a Meilisearch index based on
        its identifier or instance.

        :param obj_or_string: The `obj_or_string` parameter is either a string
        or a Django model instance that needs to be removed from the Meilisearch
        index.
        :param commit: not used
        """
        identifier = get_identifier(obj_or_string)
        index_name = self._index_name(obj_or_string)
        self.client.index(index_name).delete_document(identifier)

    def update(self, index: SearchIndex, iterable: Iterable, commit: bool = True):
        """
        Update a MeiliSearch index with new documents and updates the searchable
        attributes.

        :param index: The index parameter is an instance of a haystack search index.
        :param iterable: `iterable` is a collection of objects that need to be
        updated in the MeiliSearch index.
        :param commit: not used
        """
        index_name = get_model_ct(index.get_model()).replace(".", "_")
        documents = []
        for document in (index.full_prepare(obj) for obj in iterable):
            document["id"] = document["id"].replace(".", "_")
            documents.append(document)

        meili_index = self.client.index(index_name)
        # Explicitly set the primary key to be the "id" field.
        # This is required as meilisearch will otherwise use any field that
        # contains "id" as the primary key
        meili_index.add_documents(documents, primary_key="id")

        # Update searchable attributes
        meili_index.update_searchable_attributes(list(index.fields.keys()))

    def search(self, query_string, result_class=None, models=None, **kwargs):
        if result_class is None:
            result_class = SearchResult
        if not models:
            models = set(
                connections[self.connection_alias]
                .get_unified_index()
                .get_indexed_models()
            )

        print(query_string, result_class, kwargs)

        if result_class is None:
            result_class = SearchResult

        if len(models) == 1:
            meili_index = self.client.index(self._index_name(models.pop()))
            raw_results = meili_index.search(query_string)
        else:
            raw_results = self.client.multi_search(
                [
                    {
                        "indexUid": self._index_name(model),
                        "query": query_string,
                    }
                    for model in models
                ]
            )

        results = []
        for match in raw_results["hits"]:
            app_label, model_name, pk = match["id"].split("_")
            results.append(
                result_class(app_label=app_label, model_name=model_name, pk=pk, score=0)
            )

        return {"results": results, "hits": len(results)}


class MeiliSearchQuery(BaseSearchQuery):
    def build_query_fragment(self, field, filter_type, value):
        # TODO: Implement field & filter_type
        return str(value)


class MeiliSearchEngine(BaseEngine):
    backend = MeiliSearchBackend
    query = MeiliSearchQuery
