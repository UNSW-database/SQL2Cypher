MATCH (person:person), (visit:visit) WHERE person.PersonId = visit.PersonId MERGE (person)-[r:VISIT]->(visit)
MATCH (place:place), (visit:visit) WHERE place.PlaceId = visit.PlaceId MERGE (place)-[r:VISIT]->(visit)


MATCH (person:Person), (visit:Visit), (p:Place) WHERE person.PersonId = visit.PersonId and visit.PlaceId = p.PlaceId create (person)-[r:PERSON_VISIT{property: [visit.VisitId, visit.starttime,visit.endtime]}]->(p) return person;

MATCH (person:person{Healthstatus: "Sick"})-[r:PERSON_VISIT]->(place:place) RETURN * LIMIT 20;