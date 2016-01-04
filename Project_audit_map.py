"""
The purpose of this file is fourfold:
- audit the OSMFILE and change the variable 'mapping' to reflect the changes needed to fix 
    the unexpected street types to the appropriate ones in the expected list.    
- write the update_name function, to actually fix the street name.
- shape the data into a list of dictionaries using the shape_element function.
- save the data in a file, so that we can use mongoimport later on to import the shaped data
    into MongoDB. 

"""


import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint
import codecs
import json

OSMFILE = "mississauga_canada.osm"  #OSM map of Mississauga, Ontario, Canada

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

# The list below represents acceptable street types
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Glen", "Walk", "Ridge", "Hills",
            "Bend", "Gallops", "Heights", "End", "Hill", "Way", "Gate", "Circle",
            "Oaks", "Baseline", "Valley", "Mews", "Centre", "Downs", "Rise", "Park",
            "Loft", "Manor", "Centre", "Downs", "Rise", "Park", "Cove", "Queensway",
            "Promenade", "Collegeway", "Dell", "Kingsway", "Path", "Line",
            "Terrace", "Greenway", "Orchard", "Grove", "Glade", "Heath",
            "Abbey", "Wold", "Crossing", "Lanes", "Thicket", "Gardens",
            "Outlook", "Run", "Pines", "Point", "Millway", "Hollow", "Woods",
            "Crescent", "Woodlands", "Close", "Chase", "Row", "North", "South", "East", "West",
            "Glenn", "Wynd", "Mall", "Homestead"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    return street_types

"""
These two lines are run in order to add to the list of expected street types
and to identify the required street name changes.
"""
streets = audit(OSMFILE)
pprint.pprint(streets)

"""
The output from streets highlights the following required changes:
defaultdict(<type 'set'>, {'Churchill': set(['Winston Churchill']), 
'restaurant': set(['restaurant']), 'Foster': set(['Foster']),
'Keanegate': set(['The Keanegate']), '9': set(['Meyerside Drive, Unit 9']),
'Rd.': set(['Advance Rd.']), 'St': set(['19-300 New Toronto St'])})

We address these corrections via the mapping dictionary and update_name function.
"""

mapping = { "abbreviated": {"Rd.": "Road", "St.": "Street"},
            "incorrect": {"Foster": "Foster Crescent",
                          "Winston Churchill": "Winston Churchill Boulevard",
                          "The Keanegate": "The Keanegate Street",
                          "Meyerside Drive, Unit 9": "Meyerside Drive",
                          "restaurant": "Mississauga Road"}                           
             }

def update_name(name, mapping):
    for key in mapping["abbreviated"]: 
        if key in name:
            name = name.replace(key, mapping["abbreviated"][key])
    for key in mapping["incorrect"]:
        if key == name:
            name = name.replace(key, mapping["incorrect"][key])
    return name


"""
The shape of the data for top level tags: "node" and "way" is now transformed into a list
of dictionaries that look like this:

{
"id": "2406124091",
"type: "node",
"visible":"true",
"created": {
          "version":"2",
          "changeset":"17206049",
          "timestamp":"2013-08-03T16:43:42Z",
          "user":"linuxUser16",
          "uid":"1219059"
        },
"pos": [41.9757030, -87.6921867],  # latitude and longitude as floats
"address": {
          "housenumber": "5157",
          "postcode": "60625",
          "street": "North Lincoln Ave"
        },
"amenity": "restaurant",
"cuisine": "mexican",
"name": "La Cabana De Don Luis",
"phone": "1 (773)-271-5176"
}

- for "way" specifically:
  <nd ref="305896090"/>
  <nd ref="1719825889"/>
should be turned into
"node_refs": ["305896090", "1719825889"]
"""


lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        node['id'] = element.attrib['id']  #id key value pair
        node['type'] = element.tag # type either 'node' or 'way'
        if 'visible' in element.attrib:
            node['visible'] = element.attrib['visible']  
        #
        # Build created dictionary
        created = {}
        for key in CREATED:
            created[key] = element.attrib[key]
        node['created'] = created  # add created dictionary
        #
        # Build pos list for geoposition for nodes only, not ways
        if element.tag == "node":
            pos = []
            lat = element.attrib['lat']
            lon = element.attrib['lon']
            pos.append(float(lat))
            pos.append(float(lon))
            node['pos'] = pos  # add pos list
        #
        # Build address dictionary and process k values
        address = {}        
        for tag in element.iter("tag"):           
            kvalue = tag.attrib['k']
            vvalue = tag.attrib['v']
            if problemchars.match(kvalue) == None:  #ignore k values with problem characters
                if kvalue[0:5] == 'addr:' and ':' not in kvalue[5:]:  #ignore addr:street tags with second :
                # fix street names
                    if kvalue[0:11] == 'addr:street':
                        if vvalue == 'restaurant':
                            node['amenity'] = 'restaurant' 
                        if vvalue == 'Meyerside Drive, Unit 9':
                            address['unit'] = '9'
                        vvalue = update_name(vvalue,mapping)
                 # add pairs to address dictionary   
                    address[kvalue[5:]] = vvalue   
                elif kvalue[0:5] != 'addr:':
                    node[kvalue] = vvalue  #add pairs for k values not starting with 'addr:'
            node['address'] = address   # add address 
        #
        # Process 'nd' values for elements with 'way' as top level tag
        node_ref_list = []
        for tag in element.iter("nd"):
            node_ref_list.append(tag.attrib['ref'])
            node['node_refs'] = node_ref_list
            
        return node
    else:
        return None

# Save the data to a file that can be imported into MongoDB
def process_map(file_in, pretty = False):
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

map_data = process_map(OSMFILE, False)

pprint.pprint(map_data[0])


