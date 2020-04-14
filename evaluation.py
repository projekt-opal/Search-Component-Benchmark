from elasticsearch import Elasticsearch
import time
from typing import Tuple
import numpy as np
import scipy as sp
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from scipy import stats

es = Elasticsearch(['http://opaldata.cs.uni-paderborn.de:9200/'])  # checec with ELASTIC SEARCG
sparql = SPARQLWrapper("http://opaldata.cs.uni-paderborn.de:3030/opal/query")


######################## Fuseki Sparql Queries #####################################################################
def elastic_search_number_of_datasets(times):
    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        result.append(es.count()['count'])
        counter.append(time.time() - start_time)

    counter = np.array(counter)
    result = np.array(result)

    print('Results from ElasticSearch')
    print(stats.describe(result))
    print(stats.describe(counter))
    print('Standard Deviation', counter.std())


def sparql_search_literal_anywhere(times):
    query = """PREFIX dcat: <http://www.w3.org/ns/dcat#>
                PREFIX dct: <http://purl.org/dc/terms/>

                SELECT  (COUNT(distinct ?s) AS ?num)
                WHERE
                { GRAPH ?g {?s  a                     dcat:Dataset ; 
                        ?p                    ?o
                    FILTER isLiteral(?o)
                    FILTER contains(?o, "Paderborn")
                  }
                }"""

    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        result.append(sparql.query().convert())
        counter.append(time.time() - start_time)

    counter = np.array(counter)

    print('####' * 10)
    print('Results form Fuseki')
    print('Average runtime ', counter.mean())
    print('Standard Deviation', counter.std())

    for i in result[0]["results"]["bindings"]:
        print(i)


def sparql_search_BERLIN_in_title(times):
    query = """PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT  (COUNT(distinct ?s) AS ?num)
    WHERE{GRAPH ?g {
  ?s  a                     dcat:Dataset .
        ?s dct:title                    ?o .
    FILTER isLiteral(?o)
    FILTER contains(STR(?o), "Berlin")}}"""

    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        result.append(sparql.query().convert())
        counter.append(time.time() - start_time)

    counter = np.array(counter)
    print('####' * 10)
    print('Results form Fuseki search in title')
    print('Average runtime ', counter.mean())
    print('Standard Deviation', counter.std())
    print('####' * 10)


def sparql_search_Baustelle_in_description(times):
    query = """PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT  (COUNT(distinct ?s) AS ?num)
    WHERE{GRAPH ?g {
  ?s  a                     dcat:Dataset .
        ?s dct:description                    ?o .
    FILTER isLiteral(?o)
    FILTER contains(STR(?o), "Baustelle")}}"""

    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        result.append(sparql.query().convert())
        counter.append(time.time() - start_time)

    counter = np.array(counter)

    print('####' * 10)
    print('Results form Fuseki')
    print('Average runtime ', counter.mean())
    print('Standard Deviation', counter.std())

    for i in result[0]["results"]["bindings"]:
        print(i)


def sparql_search_Bahnhoff_in_keyword(times):
    query = """PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT  (COUNT(distinct ?s) AS ?num)
    WHERE{GRAPH ?g {
  ?s  a                     dcat:Dataset .
        ?s dcat:keyword                    ?o .
    FILTER isLiteral(?o)
    FILTER contains(STR(?o), "Bahnhoff")}}"""

    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        result.append(sparql.query().convert())
        counter.append(time.time() - start_time)

    counter = np.array(counter)

    print('####' * 10)
    print('Results form Fuseki')
    print('Average runtime ', counter.mean())
    print('Standard Deviation', counter.std())

    for i in result[0]["results"]["bindings"]:
        print(i)


def sparql_search_Berlin_Flughafen_in_description(times):
    query = """PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT  (COUNT(distinct ?s) AS ?num)
    WHERE{GRAPH ?g {
  ?s  a                     dcat:Dataset .
        ?s dct:description                    ?o .
    FILTER isLiteral(?o)
    FILTER contains(STR(?o), "Berlin Flughafen")}}"""

    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        result.append(sparql.query().convert())
        counter.append(time.time() - start_time)

    counter = np.array(counter)

    print('####' * 10)
    print('Results form Fuseki')
    print('Average runtime ', counter.mean())
    print('Standard Deviation', counter.std())

    for i in result[0]["results"]["bindings"]:
        print(i)

def sparql_query_literal_in_obj(times):
    query = """ 
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    SELECT  (COUNT(distinct ?s) AS ?num)
        WHERE
        { GRAPH ?g {?s  a                     dcat:Dataset 
        }}"""

    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        result.append(sparql.query().convert())
        counter.append(time.time() - start_time)

    counter = np.array(counter)
    result = np.array(result)

    print('Results form Fuseki')
    # print(stats.describe(result))
    print(stats.describe(counter))
    print('Standard Deviation', counter.std())


def num_of_datasets(times=None):
    elastic_search_number_of_datasets(times)
    sparql_query_literal_in_obj(times)


####################################################################################################





def elastic_search_query_in_description(times, keyword):
    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        query = {'query': {'match': {'description': keyword}}}
        result.append(es.count(body=query))
        counter.append(time.time() - start_time)

    counter = np.array(counter)
    result = np.array(result)
    print(result[0])

    print('####' * 10)
    print('Results form ES')
    print(counter.mean())
    print('Standard Deviation', counter.std())
    print('####' * 10)


def elastic_search_query_in_title(times, keyword):
    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        query = {'query': {'match': {'title': keyword}}}
        result.append(es.count(body=query)['count'])
        counter.append(time.time() - start_time)

    counter = np.array(counter)
    result = np.array(result)

    print('####' * 10)
    print('Results form ELASTIC search in Title')
    print('Average runtime ', counter.mean())
    print('Standard Deviation', counter.std())
    print('\n' * 3)


def elastic_search_query_in_keyword(times, keyword):
    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        query = {'query': {'match': {'keyword': keyword}}}
        result.append(es.count(body=query)['count'])
        counter.append(time.time() - start_time)

    counter = np.array(counter)
    print(result[0])

    print('####' * 10)
    print('Results form ELASTIC search in Title')
    print('Average runtime ', counter.mean())
    print('Standard Deviation', counter.std())
    print('\n' * 3)


def elastic_search_query_in_theme(times, keyword):
    counter = []
    result = []
    for i in range(times):
        start_time = time.time()
        query = {'query': {'match': {'themes': keyword}}}
        result.append(es.search(body=query))
        counter.append(time.time() - start_time)

    counter = np.array(counter)
    print(result[0])

    print('####' * 10)
    print('Average runtime ', counter.mean())
    print('Standard Deviation', counter.std())
    print('\n' * 3)



sparql_search_BERLIN_in_title(times=100)
elastic_search_query_in_title(times=100, keyword='Berlin')

sparql_search_Baustelle_in_description(times=100)
elastic_search_query_in_description(times=100, keyword='Baustelle')

sparql_search_Bahnhoff_in_keyword(times=100)
elastic_search_query_in_keyword(times=100, keyword='Bahnhoff')

sparql_search_Berlin_Flughafen_in_description(times=100)
elastic_search_query_in_description(times=100, keyword='Berlin Flughafen')
# elastic_search_query_in_theme(times=1, keyword='<http://publications.europa.eu/resource/authority/data-theme/EDUC>')