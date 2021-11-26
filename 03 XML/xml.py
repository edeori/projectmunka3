import xml.etree.ElementTree as et
import pandavro as pdx
import pandas as pd
import re

def convertDateTime(datetime):
    return re.sub(r'(\d{4})-(\d{1,2})-(\d{1,2})', '\\1-\\2-\\3', datetime[0:9])

def readFromXmlToDict(path):
    #read xml file
    tree = et.parse(path)
    rootNode = tree.getroot()
    
    # Collect data from xml
    data = {}
    i = 0
    for child in rootNode:
        data[i] = []
        for ch in child:
            if ch.tag == "createdAt" or ch.tag == "updatedAt":
                data[i].append(convertDateTime(ch.text))
            else:
                data[i].append(ch.text)
        i+=1
    return data

dictData = readFromXmlToDict("C:\\employee.xml")

OUTPUT_PATH = "C:\\employee.avro"

pdx.to_avro(OUTPUT_PATH, pd.DataFrame.from_dict(dictData))

#print(pd.DataFrame.from_dict(dictData))

saved = pdx.read_avro(OUTPUT_PATH)
print(saved)
