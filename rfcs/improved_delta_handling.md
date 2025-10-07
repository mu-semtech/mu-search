---
Stage: Draft
Start Date: 07-10-2025
Release Date: Unreleased
RFC PR: 
---

# improved delta handling

## Summary
This RFC propopes a new way of handling delta's. The premise of this RFC is that we can find the shape overlap between incoming triples and document shapes with SPARQL queries on the incoming triples. This would reduce the amount of queries on the triplestore and in turn increase performance.

## Motivation
The current delta handler executes many queries to determine which documents need to be updated, this could be greatly reduced by better matching incoming triples with the shapes described in the search config.


## Detailed design
The mu-search service receives delta's in the following form
```json
    [
      { "inserts": [{"subject": { "type": "uri", "value": "http://mu.semte.ch/" },
                     "predicate": { "type": "uri", "value": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" },
                     "object": { "type": "uri", "value": "https://schema.org/Project" }},
                     {"subject": { "type": "uri", "value": "http://mu.semte.ch/" },
                     "predicate": { "type": "uri", "value": "http://purl.org/dc/terms/modified" },
                     "object": { "type": "literal", "value": "https://schema.org/Project", "datatype": "http://www.w3.org/2001/XMLSchema#dateTime"}}],
        "deletes": [] }
    ]
```

Document shapes are defined in the search config, for example (truncated for brevity):
```json
{
  "types": [
    {
      "type": "session",
      "on_path": "sessions",
      "rdf_type": [
        "http://data.vlaanderen.be/ns/besluit#Zitting"
      ],
      "properties": {
        "abstract_location_id": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://data.vlaanderen.be/ns/besluit#werkingsgebied",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "location_id": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://data.vlaanderen.be/ns/besluit#werkingsgebied",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "abstract_governing_body_location_name": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://data.vlaanderen.be/ns/besluit#werkingsgebied",
          "http://www.w3.org/2000/01/rdf-schema#label"
        ],
        "governing_body_location_name": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://data.vlaanderen.be/ns/besluit#werkingsgebied",
          "http://www.w3.org/2000/01/rdf-schema#label"
        ],
        "abstract_governing_body_id": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "abstract_governing_body_name": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "abstract_governing_body_classification_name": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://www.w3.org/ns/org#classification",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "abstract_governing_body_classification_id": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://www.w3.org/ns/org#classification",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "governing_body_id": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "governing_body_name": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "governing_body_classification_name": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://www.w3.org/ns/org#classification",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "governing_body_classification_id": [
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://www.w3.org/ns/org#classification",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "planned_start": [
          "http://data.vlaanderen.be/ns/besluit#geplandeStart"
        ],
        "started_at": [
          "http://www.w3.org/ns/prov#startedAtTime"
        ],
        "ended_at": [
          "http://www.w3.org/ns/prov#endedAtTime"
        ],
        "agenda-items_id": [
          "http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "agenda-items_title": [
          "http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://purl.org/dc/terms/title"
        ],
        "agenda-items_description": [
          "http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://purl.org/dc/terms/description"
        ],
        "resolutions_title": [
          "http://data.vlaanderen.be/ns/besluit#behandelt",
          "^http://purl.org/dc/terms/subject",
          "http://www.w3.org/ns/prov#generated",
          "http://data.europa.eu/eli/ontology#title"
        ],
        "resolutions_description": [
          "http://data.vlaanderen.be/ns/besluit#behandelt",
          "^http://purl.org/dc/terms/subject",
          "http://www.w3.org/ns/prov#generated",
          "http://data.europa.eu/eli/ontology#description"
        ]
      },
    },
    {
      "type": "agenda-item",
      "on_path": "agenda-items",
      "rdf_type": [
        "http://data.vlaanderen.be/ns/besluit#Agendapunt"
      ],
      "properties": {
        "abstract_location_id": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://data.vlaanderen.be/ns/besluit#werkingsgebied",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "location_id": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://data.vlaanderen.be/ns/besluit#werkingsgebied",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "abstract_governing_body_location_name": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://data.vlaanderen.be/ns/besluit#werkingsgebied",
          "http://www.w3.org/2000/01/rdf-schema#label"
        ],
        "governing_body_location_name": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/besluit#bestuurt",
          "http://data.vlaanderen.be/ns/besluit#werkingsgebied",
          "http://www.w3.org/2000/01/rdf-schema#label"
        ],
        "abstract_governing_body_id": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "abstract_governing_body_name": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "abstract_governing_body_classification_name": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://www.w3.org/ns/org#classification",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "abstract_governing_body_classification_id": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
          "http://www.w3.org/ns/org#classification",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "governing_body_id": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "governing_body_name": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "governing_body_classification_name": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://www.w3.org/ns/org#classification",
          "http://www.w3.org/2004/02/skos/core#prefLabel"
        ],
        "governing_body_classification_id": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#isGehoudenDoor",
          "http://www.w3.org/ns/org#classification",
          "http://mu.semte.ch/vocabularies/core/uuid"
        ],
        "session_planned_start": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://data.vlaanderen.be/ns/besluit#geplandeStart"
        ],
        "session_started_at": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://www.w3.org/ns/prov#startedAtTime"
        ],
        "session_ended_at": [
          "^http://data.vlaanderen.be/ns/besluit#behandelt",
          "http://www.w3.org/ns/prov#endedAtTime"
        ],
        "title": [
          "http://purl.org/dc/terms/title"
        ],
        "description": [
          "http://purl.org/dc/terms/description"
        ],
        "resolution_title": [
          "^http://purl.org/dc/terms/subject",
          "http://www.w3.org/ns/prov#generated",
          "http://data.europa.eu/eli/ontology#title"
        ],
        "resolution_description": [
          "^http://purl.org/dc/terms/subject",
          "http://www.w3.org/ns/prov#generated",
          "http://data.europa.eu/eli/ontology#description"
        ]
      }
    }
  ]
}

```

As can be seen document shapes can be quite complex (7+ levels deep, with inverses and nested objects). Mapping these to SPARQL queries only needs to be done once. Given that this query works correctly with optionals (to be tested!), it should be possible to extract all relevant triples for a given document type in one query. We would then only need to do one query to verify changes. 

### Mapping to a query
The above config for agendapoint should result in a query like the following:

### General logic
```sparql
SELECT * WHERE { 
 # type
 OPTIONAL { ?subject a besluit:Agendapunt }
 # abstract_location_id
 OPTIONAL { ?subject ^<http://data.vlaanderen.be/ns/besluit#behandelt> ?abstract_loication_id_var2 }
 OPTIONAL { ?abstract_location_id_var2 } <http://data.vlaanderen.be/ns/besluit#isGehoudenDoor> ?abstract_location_id_var3 }

 OPTIONAL { ?abstract_location_id_var3 <http://data.vlaanderen.be/ns/besluit#bestuurt> ?abstract_loication_id_var4 }
 OPTIONAL { ?abstract_location_id_var4  <http://data.vlaanderen.be/ns/besluit#werkingsgebied> ?abstract_loication_id_var5 }
 OPTIONAL { ?abstract_location_id_var5 >http://mu.semte.ch/vocabularies/core/uuid> ?abstract_loication_id_var6 }
 # location id
 # abstract_location_id
 OPTIONAL { ?subject ^<http://data.vlaanderen.be/ns/besluit#behandelt> ?abstract_loication_id_var2 }
 OPTIONAL { ?abstract_location_id_var2 } <http://data.vlaanderen.be/ns/besluit#isGehoudenDoor> ?abstract_location_id_var3 }
  OPTIONAL { ?abstract_location_id_var3 <http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan> ?abstract_location_id_var4 }

 OPTIONAL { ?abstract_location_id_var4 <http://data.vlaanderen.be/ns/besluit#bestuurt> ?abstract_loication_id_var5 }
 OPTIONAL { ?abstract_location_id_var5  <http://data.vlaanderen.be/ns/besluit#werkingsgebied> ?abstract_loication_id_var6 }
 OPTIONAL { ?abstract_location_id_var7 >http://mu.semte.ch/vocabularies/core/uuid> ?abstract_loication_id_var8 }     
 #...
```

For each binding:
1a. If this matches a subject, just queue that and be done with it 
1b. if not: construct query to fetch the type and then queue subject and be done with it

### Inserts
It's fairly easy to reason about inserts, since we can just query for the extra data in the actual triple store.

### Deletes
Deletes are more complex, because they indicate an absence of data. If a certain match was deleted we need to rebuild that document, but how do we get the subject uri if we can no longer find the path to the subject. In theory at least all relevant info should be in the delta and the query should allow us to find it.

I'm not sure the previous single triple approach worked perfectly, but it at least worked well enough. As part of this RFC implementation we should write some tests to see if all relevant documents are found / queried for. The main difference for deletes is that queries to find additional data in the store must be written differently than for inserts. The bindings should help in writing those queries more easily.

### Folding
Folding of inserts and deletes was not done in earlier versions of mu-auth/deltanotifier but is now possible and should be assumed. This means that inserts/deletes of the same data should no longer be considered and can be ignored. This should be mentioned in the readme and config examples for deltanotifier.
