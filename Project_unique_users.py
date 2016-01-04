import xml.etree.cElementTree as ET
import pprint
import re
"""
Your task is to explore the data a bit more.
The first task is a fun one - find out how many unique users
have contributed to the map in this particular area!

The function process_map should return a set of unique user IDs ("uid")
"""

OSMFILE = "mississauga_canada.osm"  #OSM map of Mississauga, Ontario, Canada

def get_user(element):
    return


def process_map(filename):
    users = set()
    for _, element in ET.iterparse(filename):
        if element.tag == "node" or element.tag == "way" or element.tag == "relation":
            userid = element.attrib['uid']
            users.add(userid)    # add 'uid' value to set of users if unique       
    return users

users = process_map(OSMFILE)

print "Number of unique users who have contributed to the map:"
print len(users)

