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
import string
import re
import subprocess

# TODO add comma after every x entry, optimize

def parseData(file):
    with open(file) as main_file:
        count = 1
        reviews = list()
        pterms = list()
        rterms = list()
        scores = list()
        quote = '&' + 'quot' + ';'
        replace_punctuation = re.compile('[%s]' % re.escape(string.punctuation))
        for x in main_file:
            if "product/productId: " in x:
                reviews.append(str(count)+",")
                str_entry = x.replace("product/productId: ","").replace("\\","\\").replace('"',quote).strip('\n')
                reviews.append(str_entry+",")

            elif "product/title: " in x:
                str_entry = x.replace("product/title: ","").replace("\\","\\").replace('"',quote).strip('\n')
                terms = replace_punctuation.sub(" ", str_entry)
                terms = terms.split(" ")
                for term in terms:
                    if len(term)>2:
                        pterms.append(term.lower()+",")
                        pterms.append(str(count)+"\n")

                reviews.append('"'+str_entry+'",')

            elif "product/price: " in x:
                str_entry = x.replace("product/price: ","").replace("\\","\\").replace('"',quote).strip('\n')
                reviews.append(str_entry+",")

            elif "review/userId: " in x:
                str_entry = x.replace("review/userId: ","").replace("\\","\\").replace('"',quote).strip('\n')
                reviews.append(str_entry+",")

            elif "review/profileName: " in x:
                str_entry = x.replace("review/profileName: ","").replace("\\","\\").replace('"',quote).strip('\n')
                reviews.append('"'+str_entry+'",')

            elif "review/helpfulness: " in x:
                str_entry = x.replace("review/helpfulness: ","").replace("\\","\\").replace('"',quote).strip('\n')
                reviews.append(str_entry+",")

            elif "review/score: " in x:
                str_entry = x.replace("review/score: ","").replace("\\","\\").replace('"',quote).strip('\n')
                reviews.append(str_entry+",")
                scores.append(str(str_entry)+",")
                scores.append(str(count)+"\n")


            elif "review/time: " in x:
                str_entry = x.replace("review/time: ","").replace("\\","\\").replace('"',quote).strip('\n')
                reviews.append(str_entry+",")

            elif "review/summary: " in x:
                str_entry = x.replace("review/summary: ","").replace("\\","\\").replace('"',quote).strip('\n')
                terms = replace_punctuation.sub(" ", str_entry)
                terms = terms.split(" ")
                for term in terms:
                    if len(term)>2:
                        rterms.append(term.lower()+",")
                        rterms.append(str(count)+"\n")
                reviews.append('"'+str_entry+'",')

            elif "review/text: " in x:
                str_entry = x.replace("review/text: ","").replace("\\","\\").replace('"',quote).strip('\n')
                reviews.append('"'+str_entry+'"\n')
                terms = replace_punctuation.sub(" ", str_entry)
                terms = terms.split(" ")
                for term in terms:
                    if len(term)>2:
                        rterms.append(term.lower()+",")
                        rterms.append(str(count)+"\n")

                count+=1

    with open("pterms.txt","a") as pterms_file:
        for entry in pterms:
            pterms_file.write(entry)

    with open("scores.txt","a") as scores_file:
        for entry in scores:
            scores_file.write(str(entry))

    with open("rterms.txt","a") as rterms_file:
        for entry in rterms:
            rterms_file.write(str(entry))

    with open("reviews.txt","a") as reviews_file:
        for entry in reviews:
            reviews_file.write(entry)


    return ['pterms.txt','scores.txt','rterms.txt']
