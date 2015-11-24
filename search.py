from query_parser import queryData
import datetime
from bsddb3 import db
#Get an instance of BerkeleyDB
reviews = db.DB()
pterms = db.DB()
rterms = db.DB()
scores = db.DB()
reviews.open("rw.idx")
pterms.open("pt.idx")
rterms.open("rt.idx")
scores.open("sc.idx")

reviewsCursor = reviews.cursor()
ptermsCursor = pterms.cursor()
rtermsCursor = rterms.cursor()
lowerScoresCursor = scores.cursor()
higherScoresCursor = scores.cursor()


#search = input("input querey u fuk: ")
#return (pterms,rterms,pprice,rscore,rdate,part_terms,terms)
search = "pprice < 60 pprice > 30 clothing rscore < 5 rscore > 4  r:funchuck p:cow chron%  "


parsedSearch = queryData(search)
termLengthTable = []
print(parsedSearch)
# make a list to check the number of terms in the parsedSearch
termLengthTable.append(len(parsedSearch[0]))    # length of pterms
termLengthTable.append(len(parsedSearch[1]))    # length of rterms
termLengthTable.append(len(parsedSearch[5]))    # length of part_terms
termLengthTable.append(len(parsedSearch[6]))    # length of terms
resultIDs1 = []  # contains all the (binary) IDs of valid results
index = 0
for length in termLengthTable:
    if index == 0 and length != 0:  # if pterms has something, search in pterms.idx only
        ptermsLength = termLengthTable[0]
        for ptermsIndex in range (0, ptermsLength):
            encodedTerm = (parsedSearch[0][ptermsIndex]).encode()
            iter = ptermsCursor.get(encodedTerm, db.DB_SET_RANGE)   # move cursor to the first item that has the term
            while iter[0] == encodedTerm:
                resultIDs1.append(iter[1])
                iter = ptermsCursor.next()
    elif index == 1 and length !=0:   # if rterms has something, search in rterms.idx only
        rtermsLength = len(parsedSearch[1])
        for rtermsIndex in range(0, rtermsLength):
            encodedTerm = (parsedSearch[1][rtermsIndex]).encode()
            iter = rtermsCursor.get(encodedTerm, db.DB_SET_RANGE)
            while iter[0] == encodedTerm:
                resultIDs1.append(iter[1])
                iter = rtermsCursor.next()
    elif index == 2 and length != 0:  # if part_terms has something, search for partial matching strings in pterms.idx and rterms.idx
        part_termsLength = len(parsedSearch[5])
        for part_termsIndex in range(0, part_termsLength):
            term = parsedSearch[5][part_termsIndex].strip("%")
            encodedTerm = term.encode()
            #encodedTerm = (parsedSearch[5][part_termsIndex]).encode()
            iter = rtermsCursor.get(encodedTerm, db.DB_SET_RANGE)   #search in rterms
            while (iter[0].decode()).startswith(term, 0, len(term)):
                resultIDs1.append(iter[1])
                iter = rtermsCursor.next()
            iter = ptermsCursor.get(encodedTerm, db.DB_SET_RANGE)   #search in pterms
            while (iter[0].decode()).startswith(term, 0, len(term)):
                resultIDs1.append(iter[1])
                iter = ptermsCursor.next()
    elif index == 3 and length != 0:    # if terms has something, search for the string in both pterms.idx and rterms.idx
        termsLength = termLengthTable[3]
        for termsIndex in range(0, termsLength):
            encodedTerm = parsedSearch[6][termsIndex].encode()
            iter1 = ptermsCursor.get(encodedTerm, db.DB_SET_RANGE)   # move cursor to the first item that has the term
            while iter1[0] == encodedTerm:
                resultIDs1.append(iter1[1])
                iter1 = ptermsCursor.next()
            iter2 = rtermsCursor.get(encodedTerm, db.DB_SET_RANGE)
            while iter2[0] == encodedTerm:
                resultIDs1.append(iter2[1])
                iter2 = rtermsCursor.next()
    index+=1
    # all possible IDs should be found - do range searching now to narrow results
resultIDs1 = list(set(resultIDs1))  # remove duplicates shitty way
resultIDs1 = [ID for ID in resultIDs1 if ID is not None] # remove None types

termLengthTable = []
print("term search", resultIDs1)

termLengthTable.append(len(parsedSearch[2]))    # length of pprice
termLengthTable.append(len(parsedSearch[3]))    # length of rscore
termLengthTable.append(len(parsedSearch[4]))    # length of rdate

#get all the valid ids and merge both lists if there are any
index = 0
validLowerIDs = list()
validHigherIDs = list()
results = list()
for amount in range(0, termLengthTable[1]):
    signChecked = parsedSearch[3][index][1]
    numberChecked = (parsedSearch[3][index][2])+".0" # checking ascii...decimals greater than bare
    if signChecked == "<":    #less than search. gets all the items from beginning up to the number
        lowerIter = lowerScoresCursor.first()
        #print(lowerIter[0].decode())
        while lowerIter is not None and lowerIter[0].decode() < numberChecked:
            validLowerIDs.append(lowerIter[1])
            lowerIter = lowerScoresCursor.next()
    elif signChecked == ">":  #greater than search. gets all the items from end down to the number
        upperIter = higherScoresCursor.last()
        while upperIter is not None and upperIter[0].decode() > numberChecked:
            validHigherIDs.append(upperIter[1])
            upperIter=higherScoresCursor.prev()
    index+=1

if termLengthTable[1] < 2:
    if len(validLowerIDs) > len(validHigherIDs):
        results = validLowerIDs
    else:
        results = validHigherIDs
else:
    for ID in validLowerIDs and validHigherIDs:    #merge tables, valid ids in results
            results.append(ID)
print("number search", results)
resultIDs2 = []
for ID in resultIDs1 and results:       # merge tables
    resultIDs2.append(ID)

#print(resultIDs2)
'''
result = reviews.get(b'5')
print(result)
#iter = reviewsCursor.first()
for encodedID in resultIDs2:
    print("\n")
    print(encodedID)
    iter = reviews.get(encodedID, db.DB_SET)
    while iter:
        print(iter)
        iter = reviewsCursor.next()
    #print(iter)
'''

# datetime conversion
datetime.datetime.strptime("2013/02/14","%Y/%m/%d").timestamp() #to timestamp format
datetime.datetime.fromtimestamp(int(1369029600)).strftime("%Y/%m/%d") # from timestamp format


'''
iter = lowerScoresCursor.first()
while(iter):
    print(iter)
    iter = lowerScoresCursor.next()
'''
'''
iter = reviewsCursor.first()
while iter:
    print("\n")
    a=iter[0][0]
    print(a)
    iter = reviewsCursor.next()
'''
#reviewsCursor.close()




#print(iter)
#while iter:
# print(type(iter))
# iter = ptermsCursor.next()
#cur.close()


def parse_reviews(reviews):
    for result in reviews:
        decode_result = result.decode()
        decode_result = decode_result.split(",")
        print("Entry:" + decode_result[0])
        print("Product ID:" + decode_result[1])
        print("Product Title:" + decode_result[2])
        print("Product Price:" + decode_result[3])
        print("UserID:" + decode_result[4])
        print("Profile Name:" + decode_result[5])
        print("Helpfulness:" + decode_result[6])
        print("Score:" + decode_result[7])
        print("Review Time:" + datetime.datetime.fromtimestamp(int(decode_result[8])).strftime("%Y/%m/%d"))
        print("Review Summary:" + decode_result[9])
        print("Review Text:" + decode_result[10])
        print("------------")

#parse_reviews(reviewsCursor.first())

reviewsCursor.close()

reviews.close()
pterms.close()
rterms.close()
scores.close()
