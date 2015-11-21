from query_parser import queryData

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
scoresCursor = scores.cursor()


#search = input("input querey u fuk: ")
#return (pterms,rterms,pprice,rscore,rdate,part_terms,terms)
search = "pprice < 60 pprice > 30 clothing rscore < 3 r:funchuck p:cow blue%"

parsedSearch = queryData(search)
termLengthTable = []
print(parsedSearch)
# make a list to check the number of terms in the parsedSearch
termLengthTable.append(len(parsedSearch[0]))    # length of pterms
termLengthTable.append(len(parsedSearch[1]))    # length of rterms
termLengthTable.append(len(parsedSearch[5]))    # length of part_terms
termLengthTable.append(len(parsedSearch[6]))    # length of terms
print(termLengthTable)
resultIDs = []  # contains all the (binary) IDs of valid results
index = 0
for length in termLengthTable:
    if index == 0 and length != 0:  # if pterms has something, search in pterms.idx only
        ptermsLength = termLengthTable[0]
        for ptermsIndex in range (0, ptermsLength):
            encodedTerm = (parsedSearch[0][ptermsIndex]).encode()
            resultIDs.append(pterms.get(encodedTerm))
    elif index == 1 and length !=0:   # if rterms has something, search in rterms.idx only
        rtermsLength = len(parsedSearch[1])
        for rtermsIndex in range(0, rtermsLength):
            encodedTerm = (parsedSearch[1][rtermsIndex]).encode()
            resultIDs.append(rterms.get(encodedTerm))
    elif index == 2 and length != 0:  # if part_terms has something, search for partial matching strings in pterms.idx and rterms.idx
        part_termsLength = len(parsedSearch[5])
        for part_termsIndex in range(0, part_termsLength):
            term = parsedSearch[5][part_termsIndex].strip("%")
            encodedTerm = term.encode()
            #encodedTerm = (parsedSearch[5][part_termsIndex]).encode()
            resultIDs.append(rtermsCursor.get(encodedTerm, db.DB_SET_RANGE))
    elif index == 3 and length != 0:    # if terms has something, search for the string in both pterms.idx and rterms.idx
        termsLength = termLengthTable[3]
        for termsIndex in range(0, termsLength):
            encodedTerm = parsedSearch[6][termsIndex].encode()
            resultIDs.append(pterms.get(encodedTerm))
            resultIDs.append(rterms.get(encodedTerm))
    index+=1
print(resultIDs)


#iter = reviewsCursor.first()
#while iter:
#    print(type(iter))
#    iter = reviewsCursor.next()
#ptermsCursor.close()


#print(iter)
#while iter:
# print(type(iter))
# iter = ptermsCursor.next()
#cur.close()

reviews.close()
pterms.close()
rterms.close()
scores.close()
