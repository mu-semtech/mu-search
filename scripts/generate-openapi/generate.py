#!/usr/bin/env python3
import os
import json

es_type_to_openapi_type = {
    "text": "string",
    "keyword": "string",
    "date": "string",
    "integer": "integer"
}

STATIC_PARAMETERS = [
    {
        "in": "query",
        "name": "page",
        "description": "For more info, read the mu-search documentation on [Pagination](https://github.com/mu-semtech/mu-search#pagination).",
        "explode": True,
        "style": "deepObject",
        "schema": {
            "type": "object",
            "properties": {
                "size": {
                    "type": "integer"
                },
                "number": {
                    "type": "integer"
                },
            }
        }
    },
    {
        "in": "query",
        "name": "collapse_uuids",
        "description": "For more info, read the mu-search documentation on [Removing duplicate results](https://github.com/mu-semtech/mu-search#removing-duplicate-results).",
        "explode": True,
        "style": "form",
        "schema": {
            "type": "string",
            "enum": [ "t" ]
        }
    },
]

def search_conf_props_to_openapi_props(s_conf_props):
    openapi_props = {}
    for key, props in s_conf_props.items():
        openapi_props[key] = {"type": es_type_to_openapi_type[props["type"]]}
    return openapi_props

def search_conf_props_to_openapi_filter_qp(s_conf_props):
    openapi_qp = {
        "in": "query",
        "name": "filter",
        "explode": True,
        "style": "deepObject",
        "schema": {
            "type": "object",
            "properties": {}
        }
    }
    for key, props in s_conf_props.items():
        openapi_qp["schema"]["properties"][key] = {"type": es_type_to_openapi_type[props["type"]]}
    return openapi_qp

if __name__ == '__main__':
    with open("/data/app/config/search/config.json"
              ) as mu_search_config_file:
        mu_search_config = json.load(mu_search_config_file)

    openapi = {
        "openapi": "3.0.0",
        "info": {
            "title": "Mu-search api description",
            "version": "Our version"
        },
        "paths": {}
    }

    paths_obj = {}
    for type in mu_search_config["types"]:
        path = f"/{type['type']}/search"
        jsonapi_properties = {}
        paths_obj[path] = {
            "get": {
                "description": f"Search the {type['type']} index",
                "responses": {
                    "200": {
                        "description": "Search results",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {
                                            "type": "object",
                                            "properties": {
                                                "id": {
                                                    "type": "string"
                                                },
                                                "type": {
                                                    "type": "string"
                                                },
                                                "attributes": {
                                                    "type":
                                                    "object",
                                                    "properties": search_conf_props_to_openapi_props(type["mappings"]["properties"]),
                                                }
                                            },
                                        },
                                    },
                                }
                            }
                        }
                    }
                }
            },
            "parameters": []
        }
        paths_obj[path]["parameters"].append({
            "in": "query",
            "name": "filter",
            "description": """Note that more filtering options exist than what it expressable in Openapi.
    Read the mu-search documentation on [Supported search methods](https://github.com/mu-semtech/mu-search#supported-search-methods) for more info""",
            "explode": True,
            "style": "deepObject",
            "schema": {
                "type": "object",
                "properties": search_conf_props_to_openapi_props(type["mappings"]["properties"])
            }
        })
        
        sort_props = search_conf_props_to_openapi_props(type["mappings"]["properties"])
        for k, v in sort_props.items():
            v["enum"] = ["asc", "desc"]
        paths_obj[path]["parameters"].append({
            "in": "query",
            "name": "sort",
            "description": "For more info, read the mu-search documentation on [sorting](https://github.com/mu-semtech/mu-search#sorting).",
            "explode": True,
            "style": "deepObject",
            "schema": {
              "type": "object",
              "properties": sort_props
            }
        })
        paths_obj[path]["parameters"].extend(STATIC_PARAMETERS)

    openapi["paths"] = paths_obj


    try:
        os.mkdir("/data/app/doc")
    except FileExistsError:
        pass
    with open("/data/app/doc/search-openapi.json", "w") as file:
        json.dump(openapi, file, indent=2)
