from setuptools import setup

setup(
    name="haystack-meilisearch",
    version="0.1",
    description="MeiliSearch backend for Haystack",
    packages=["haystack_meilisearch"],
    install_requires=["meilisearch", "django-haystack"],
)
