"""
Use  iterative parsing to process the map file and
find out not only what tags are there, but also how many, to get the
feeling on how much of which data we can expect to have in the map.
The count_tags function returns a dictionary with the 
tag name as the key and number of times this tag can be encountered in 
the map as value.
"""
import xml.etree.cElementTree as ET
import pprint

OSMFILE = "mississauga_canada.osm"  #OSM map of Mississauga, Ontario, Canada

def count_tags(filename):
    dict = {}   # initialize dictionary of tags and occurrences
    for event, elem in ET.iterparse(filename):
        if event == 'end':
            if elem.tag in dict:
                dict[elem.tag] +=1  #increment tag count by 1 
            else:
                dict[elem.tag] = 1  #add tag to dict
    pprint.pprint(dict)
    return dict           

print "The number and type of tags for the OSM file of"
print "Mississauga, Ontario, Canada are as follows:"
count_tags(OSMFILE)

