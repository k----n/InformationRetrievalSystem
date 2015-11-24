#
#   Copyright 2015 Kalvin Eng
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
import re
import search

def queryData(string):
    pterms = list()
    rterms = list()
    pprice = list()
    rscore = list()
    rdate = list()
    part_terms = list()
    terms = list()

    find_pterms = re.findall("p:(\w+)", string)

    if find_pterms:
        pterms+=find_pterms

    string = re.sub("p:(\w+)", "", string)

    find_rterms = re.findall("r:(\w+)", string)

    if find_rterms:
        rterms+=find_rterms

    string = re.sub("r:(\w+)", "", string)

    find_compare = re.findall("(\w+)\s*([<>])+\s*([\w/]+)", string)

    if find_compare:
        for query in find_compare:
            if "pprice" in query:
                pprice.append(query)
            elif "rscore" in query:
                rscore.append(query)
            elif "rdate" in query:
                rdate.append(query)

    string = re.sub("(\w+)\s*([<>])+\s*([\w/]+)","", string)

    find_part_terms = re.findall("\w+%", string)

    if find_part_terms:
        part_terms+=find_part_terms

    string = re.sub("\w+%","", string)

    find_terms = re.findall("\w+", string)

    if find_terms:
        terms+=find_terms

    return (pterms,rterms,pprice,rscore,rdate,part_terms,terms)

if __name__ == "__main__":
    print("Enter nothing to exit")
    while True:
        query = input("query: ")
        if query == "":
            break
        processed_query = queryData(query)
        print(search.termsFilter(processed_query))
