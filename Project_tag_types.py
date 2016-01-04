import xml.etree.cElementTree as ET
import pprint
import re
"""
This file checks the "k" value for each "<tag>" and sees if each can be valid keys in MongoDB,
as well as see if there are any other potential problems.

There are 3 regular expressions to check for certain patterns
in the tags. The goal is to change the data model
and expand the "addr:street" type of keys to a dictionary like this:
{"address": {"street": "Some value"}}
So, we have to see if we have such tags, and if we have any tags with problematic characters.
"""


lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

OSMFILE = "mississauga_canada.osm"  #OSM map of Mississauga, Ontario, Canada

def key_type(element, keys):
    if element.tag == "tag":
        kvalue = element.attrib['k']   #'k' value
        if lower.match(kvalue):
            keys['lower'] += 1
        elif lower_colon.match(kvalue):
            keys['lower_colon'] += 1
        elif problemchars.match(kvalue):
            keys['problemchars'] += 1
        else:
            keys['other'] += 1
        
    return keys



def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    pprint.pprint(keys)
    return keys

print "Format for k values of the tags to ensure they can be valid keys in MongoDB:"
process_map(OSMFILE)

