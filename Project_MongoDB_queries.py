#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This file will run various queries against the mississauga_canada.osm.json
file that has been imported into MongoDB using the following command
(issued from the C:\Program Files\MongoDB\Server\3.0\bin folder):
mongoimport -d project -c maps --file mississauga_canada.osm.json

Size of files:
06/25/2015  12:48 AM       145,071,175 mississauga_canada.osm
07/22/2015  06:05 PM       161,117,737 mississauga_canada.osm.json

"""

import re
import pprint


def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.project
    return db


if __name__ == "__main__":

    db = get_db()
    # General information about the documents
    documents = db.maps.find()
    print "Number of documents: ", documents.count()
    nodes = db.maps.find({"type":"node"})
    print "Number of nodes: ", nodes.count()
    ways = db.maps.find({"type":"way"})
    print "Number of ways: ", ways.count()
    users =db.maps.distinct("created.user")
    print "Number of unique users: ", len(users)

    top_3_users = db.maps.aggregate([{"$group":{"_id":"$created.user",
                                              "count":{"$sum":1}}},
                                      {"$sort":{"count":-1}},
                                      {"$limit": 3}])
    
    print "Top three contributing users:"
    for doc in top_3_users:
        pprint.pprint(doc)

    top_10_users = db.maps.aggregate([{"$group":{"_id":"$created.user",
                                              "count":{"$sum":1}}},
                                      {"$sort":{"count":-1}},
                                      {"$limit": 10}])

    for dict in top_10_users:
        contr_ratio = round(float(dict["count"])/ documents.count() * 100, 2)
        print "User",dict["_id"],"has contributed",contr_ratio,"%"

    print "Number of users with only one entry"
    one_entry = db.maps.aggregate([{"$group":{"_id":"$created.user",
                                              "count":{"$sum":1}}},
                                    {"$group": {"_id":"$count",
                                               "num_users":{"$sum":1}}},
                                   {"$sort":{"_id":1}},
                                   {"$limit": 1}])
    for doc in one_entry:
        pprint.pprint(doc)

    # nodes with websites

 #   query = db.maps.find({"contact:website":{"$exists": 1}})

 #   for dict in query:
    #    pprint.pprint(dict["contact:website"])

    # Fix error with website by removing field
    db.maps.update({"contact:website":"+1 905-891-9300"},
                   {"$unset":{"contact:website":""}})  

    web_check = db.maps.find({"pos":[43.5711473, -79.5651187]})
    for doc in web_check:
        pprint.pprint(doc)  #verify field has been removed

    # Fix remaining error with street name
    error = db.maps.find({"address.street":"1134"})
    print "This document had an incorrect value for 'street' field:"
    for doc in error:
        pprint.pprint(doc)

    db.maps.update({"address.street":"1134"},
                   {"$unset":{"address":" "}})  #remove this street field
    doc_check = db.maps.find({"pos":[43.5329477, -79.6110812]})
    for doc in doc_check:
        pprint.pprint(doc)  #verify street field has been removed
   
    # Check postal code format
    postcode = db.maps.find({"address.postcode":{"$exists":1}})
    print "Number of documents with postal codes:", postcode.count()

   
    # Print all postal codes and look for format inconsistencies

  #  db.maps.update({"address.postcode":"$address.postcode[:3] $address.postcode[3:]"},
      #             {"$set":{"address.postcode":"L5M1L9"}})
    
  #  for code in postcode:
   #     pprint.pprint(code["address"]["postcode"])

   # Postal codes have inconsistent formats: A1A 1A1, A1A1A1 and one with A1A
   
    entry = db.maps.find({"address.postcode":
                          {"$regex":"[A-Z][0-9][A-Z][0-9][A-Z][0-9]"}})
    print "Number of documents with postal codes in format A1A1A1:", entry.count()
    entry2 = db.maps.find({"address.postcode":
                          {"$regex":"[A-Z][0-9][A-Z]\s[0-9][A-Z][0-9]"}})
    print "Number of documents with postal codes in format A1A 1A1:", entry2.count()

    # Fix postal code entries
    bad_pc = []   # list of postal codes with format A1A1A1
    for doc in entry:
        bad_pc.append(doc["address"]["postcode"])

    for item in bad_pc:
        db.maps.update({"address.postcode": item},
                   {"$set":{"address.postcode":item[:3] + " " + item[3:]}})
    
    print "Number of documents with postal codes in format A1A1A1:", entry.count()
    print "Number of documents with postal codes in format A1A 1A1:", entry2.count()

    # Check for addresses that contain a state field rather than a province field
    # Note that Canada doesn't have states, only provinces
    state = db.maps.find({"address.state":{"$exists":1}})

    print "Number of documents with addresses containing a field for state (should be province): ", state.count()
    province = db.maps.find({"address.province":{"$exists":1}})

    print "Number of documents with addresses containing a field for province: ", province.count() 

    # Update all "address.state" fields to "address.province".

 #   db.maps.update({"address.state":{"$exists":1}},
   #                {"$set":{"address.province":"ON"}},
  #                 multi=True)
 #   db.maps.update({"address.state":{"$exists":1}},                   
  #                 {"$unset":{"address.state":""}},
  #                 multi=True)

#    print state.count()
 #   print province.count()

    # Check for documents that have an amenity field
    amenity = db.maps.find({"amenity":{"$exists":1}})
    print "Number of documents with the amenity field: ", amenity.count()
  #  from sets import Set
  #  amenity_type = Set([])
  #  for doc in amenity:
   #     amenity_type.add(doc["amenity"])
 #   pprint.pprint(amenity_type)
            
    # Check nodes that contain an address unit field to look for incomplete addresses
    # missing one or more of housenumber, street, city, state/province, postal code
    unit = db.maps.find({"address.unit":{"$exists":1}})
    count = 0
    for doc in unit:
        if len(doc["address"]) <= 5:
           # pprint.pprint(doc["address"])
            count += 1
    print "There are",count,"incomplete node addresses that contain the unit field"
            
    # Locations of coffee places
    cost = db.maps.find({"cost:coffee":{"$exists":1}})
    print "There are",cost.count(),"locations identified that sell coffee."
    count = 0
    for doc in cost:
        if doc["cost:coffee"] == "$1.5":
            count += 1
    print count,"of these places sell coffee for $1.50."

    # HOV lanes
    query2 = db.maps.find({"hov:lanes":{"$exists": 1}})

    for dict in query2:
        pprint.pprint(dict["hov:lanes"])

    # Top appearing amenities
    print "Top appearing amenities:"

    top_amenities = db.maps.aggregate([{"$match":{"amenity":{"$exists":1}}},
                                       {"$group":{"_id":"$amenity",
                                        "count":{"$sum":1}}},
                                       {"$sort":{"count":-1}},
                                       {"$limit":10}])

    for doc in top_amenities:
        pprint.pprint(doc)
                                        
    # Top fast food restaurants
    print "Top types of fast food restaurants:"
    top_fast_food = db.maps.aggregate([{"$match":{"amenity":{"$exists":1},
                                                  "amenity":"fast_food",
                                                  "cuisine":{"$exists":1}}},
                                       {"$group":{"_id":"$cuisine",
                                                  "count":{"$sum":1}}},
                                       {"$sort":{"count":-1}},
                                       {"$limit":3}])

    for doc in top_fast_food:
        pprint.pprint(doc)

    # Different types of banks
    print "Different types of banks"
    banks = db.maps.aggregate([{"$match":{"amenity":{"$exists":1},
                                         "amenity":"bank"}},
                              {"$group":{"_id":"$name",
                                         "count":{"$sum":1}}},
                              {"$sort":{"count":-1}}])

    for doc in banks:
        pprint.pprint(doc)

    # Different Christian denominations
    print "Top 5 Christian denominations"
    worship = db.maps.aggregate([{"$match":{"amenity":{"$exists":1},
                                            "amenity":"place_of_worship",
                                            "religion":"christian",
                                            "denomination":{"$exists":1}}},
                              {"$group":{"_id":"$denomination",
                                         "count":{"$sum":1}}},
                              {"$sort":{"count":-1}},
                                 {"$limit":5}])

    for doc in worship:
        pprint.pprint(doc)

'''

    # Create a set of all unique keys for nodes
    from sets import Set
    node_keys = Set([])
    nodes = db.maps.find({"type":"node"})
    for doc in nodes:
        for key in doc:
            node_keys.add(key)
    print "Here are all the different fields for nodes:"
    pprint.pprint(node_keys)


    # Create a set of all unique keys for ways
 #   way_keys = Set([])
 #   ways = db.maps.find({"type":"way"})
 #   for doc in ways:
  #      for key in doc:
  #          way_keys.add(key)
    print "Here are all the different fields for ways:"
   # pprint.pprint(way_keys)


   




'''
    

