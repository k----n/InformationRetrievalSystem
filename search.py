from query_parser import queryData
import datetime
from bsddb3 import db
from csv import reader
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
#search = "pprice < 60 pprice > 30 clothing rscore < 5 rscore > 4.0  r:funchuck p:cow chron%  rdate > 2004/10/03 rdate < 2010/01/01"
search = "another rdate > 2010/01/01 pprice > 10"
#search = 'another rdate > 2010/01/01 pprice > 10 pprice < 60'
#search = "cam%"

parsedSearch = queryData(search)
def termsFilter(parsedSearch):
    termLengthTable = []
    print(parsedSearch)
    # make a list to check the number of terms in the parsedSearch
    termLengthTable.append(len(parsedSearch[0]))    # length of pterms
    termLengthTable.append(len(parsedSearch[1]))    # length of rterms
    termLengthTable.append(len(parsedSearch[5]))    # length of part_terms
    termLengthTable.append(len(parsedSearch[6]))    # length of terms
    #resultIDs = list()  # contains all the (binary) IDs of valid results
    resultIDs1 = list()
    resultIDs2 = list()
    resultIDs3 = list()
    resultIDs4 = list()
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
                    resultIDs2.append(iter[1])
                    iter = rtermsCursor.next()
        elif index == 2 and length != 0:  # if part_terms has something, search for partial matching strings in pterms.idx and rterms.idx
            part_termsLength = len(parsedSearch[5])
            for part_termsIndex in range(0, part_termsLength):
                term = parsedSearch[5][part_termsIndex].strip("%")
                encodedTerm = term.encode()
                #encodedTerm = (parsedSearch[5][part_termsIndex]).encode()
                iter = rtermsCursor.get(encodedTerm, db.DB_SET_RANGE)   #search in rterms
                while (iter[0].decode()).startswith(term, 0, len(term)):
                    resultIDs3.append(iter[1])
                    iter = rtermsCursor.next()
                iter = ptermsCursor.get(encodedTerm, db.DB_SET_RANGE)   #search in pterms
                while (iter[0].decode()).startswith(term, 0, len(term)):
                    resultIDs3.append(iter[1])
                    iter = ptermsCursor.next()
        elif index == 3 and length != 0:    # if terms has something, search for the string in both pterms.idx and rterms.idx
            termsLength = termLengthTable[3]
            for termsIndex in range(0, termsLength):
                encodedTerm = parsedSearch[6][termsIndex].encode()
                iter1 = ptermsCursor.get(encodedTerm, db.DB_SET_RANGE)   # move cursor to the first item that has the term
                while iter1[0] == encodedTerm:
                    resultIDs4.append(iter1[1])
                    iter1 = ptermsCursor.next()
                iter2 = rtermsCursor.get(encodedTerm, db.DB_SET_RANGE)
                while iter2[0] == encodedTerm:
                    resultIDs4.append(iter2[1])
                    iter2 = rtermsCursor.next()
        index+=1
        # all possible IDs should be found - do range searching now to narrow results
    completeList = [resultIDs1, resultIDs2, resultIDs3, resultIDs4]
    resultIDs = set(resultIDs1 or resultIDs2 or resultIDs3 or resultIDs4)
    #print(completeList)
    for results in completeList:
        if len(results) > 0:
            #print(results)
            resultIDs = resultIDs & set(results)
    return resultIDs
#print(resultIDs)
#resultIDs1 = [ID for ID in resultIDs1 if ID is not None] # remove None types
#resultIDs1 = set(resultIDs1)  # remove duplicates shitty way
#print(len(resultIDs1))
#print(resultIDs1)

#print("term search", resultIDs1)

termLengthTable = []
termLengthTable.append(len(parsedSearch[2]))    # length of pprice
termLengthTable.append(len(parsedSearch[3]))    # length of rscore
termLengthTable.append(len(parsedSearch[4]))    # length of rdate

#get all the valid ids and merge both lists if there are any
index = 0
validLowerIDs = list()
validHigherIDs = list()
scoreResults = set()
for amount in range(0, termLengthTable[1]):
    signChecked = parsedSearch[3][index][1]
    numberChecked = float(parsedSearch[3][index][2]) # checking ascii...decimals greater than bare
    if signChecked == "<":    #less than search. gets all the items from beginning up to the number
        lowerIter = lowerScoresCursor.first()
        #print(lowerIter[0].decode())
        while lowerIter is not None and float(lowerIter[0].decode()) < numberChecked:
            validLowerIDs.append(lowerIter[1])
            lowerIter = lowerScoresCursor.next()
    elif signChecked == ">":  #greater than search. gets all the items from end down to the number
        upperIter = higherScoresCursor.last()
        while upperIter is not None and float(upperIter[0].decode()) > numberChecked:
            validHigherIDs.append(upperIter[1])
            upperIter=higherScoresCursor.prev()
    index+=1
'''
if termLengthTable[1] < 2:  # no range search
    if len(validLowerIDs) > len(validHigherIDs):
        scoreResultsesults = validLowerIDs
    else:
        scoreResults = validHigherIDs
else:                       # range search
    for ID in validLowerIDs and validHigherIDs:    #merge tables, valid ids in results
            scoreResults.append(ID)

#print("number search", results)
resultIDs2 = []
if termLengthTable[1] < 0:
    for ID in resultIDs1 and scoreResults:       # merge tables
        resultIDs2.append(ID)
    resultIDs2 = list(set(resultIDs2))
else:
    resultIDs2 = resultIDs1
'''
if termLengthTable[1] > 0:
    scoreResults = resultIDs & set(validLowerIDs)
    scoreResults = scoreResults & set(validHigherIDs)
else:
    scoreResults = resultIDs

#print("score",scoreResults)

#print(resultIDs2)

index = 0 # index is terms in parsed search under rdate, 0 = no dates, 1 = 1 date, 2 = range of dates
validLowerDateIDs = list()
validHigherDateIDs = list()
dateResults = list()
if (termLengthTable[2]) > 0:   #there is some rdate criterias
    while (index < termLengthTable[2]):
        for encodedID in scoreResults:
            iter = reviewsCursor.first()
            iter = reviews.get(encodedID, db.DB_SET)
            iter = iter.decode()
            iter = [entry for entry in reader([iter])][0]
            dateTerm = parsedSearch[4][index]
            extractedDate = str(datetime.datetime.strptime(parsedSearch[4][index][2],"%Y/%m/%d").timestamp())  #date of input
            #print(datetime.datetime.fromtimestamp(int(iter[7])).strftime("%Y/%m/%d"))
            #print("date",iter[7])
            currentDate = iter[7]   #date of data
            if dateTerm[1] == '<':
                #while iter is not None:
                #    currentDate = iter[7]
                    if currentDate < extractedDate:
                        #print(dates[2])
                        #print(encodedID)
                        dateResults.append(encodedID)
                #    iter = reviewsCursor.next()

                        #print(iter)
                        #print(datetime.datetime.fromtimestamp(int(iter[7])).strftime("%Y/%m/%d"))
                        #print(iter[0])
            if dateTerm[1] == '>':
                #while iter is not None:
                    #currentDate = iter[7]
                    #print(iter[1])
                    if currentDate > extractedDate:
                        #print(encodedID)
                        dateResults.append(encodedID)
                #    iter = reviewsCursor.next()
                    #print ("zero",iter[2])
                    #print(iter)
                        #print(iter)
                        #print(datetime.datetime.fromtimestamp(int(iter[7])).strftime("%Y/%m/%d"))
                        #print(iter[0])
        index += 1
'''
if termLengthTable[2] < 0:
    if termLengthTable[2]<2: #no range search
        if len(validLowerDateIDs) > len(validHigherDateIDs):
            dateResults = validLowerDateIDs
        else:
            dateResults = validHigherDateIDs
    else:                       # range search
        for ID in validLowerDateIDs and validHigherDateIDs:    #merge tables, valid ids in dateResults
            dateResults.append(ID)
else:
    dateResults = resultIDs2
'''
#print("date",dateResults)
if termLengthTable[2] > 0:
    dateResults = (set(dateResults))
    dateResults = dateResults & scoreResults
else:
    dateResults = scoreResults
#print("date", dateResults)


ppriceIndex = 0
validLowerPriceIDs = list()
validHigherPriceIDs = list()
priceResults = list()
if (termLengthTable[0])>0: #there are some pprice criteria
    while (ppriceIndex < termLengthTable[0]):
        for encodedID in dateResults:
            iter = reviewsCursor.first()
            iter = reviews.get(encodedID, db.DB_SET)
            iter = iter.decode()
            iter = [entry for entry in reader([iter])][0]
            priceTerm = parsedSearch[2][ppriceIndex]
            checkPrice = priceTerm[2]
            #print(checkPrice)
            currentPrice = iter[2]
            if priceTerm[1] == '<':
                #currentPrice = iter[2]
                #while iter is not None:
                    if currentDate != 'unknown' and currentDate < checkPrice:
                        priceResults.append(encodedID)
                #    iter = reviewsCursor.next()
                    #checkPrice = priceTerm[2]
            if priceTerm[1] == '>':
                #while iter is not None:
                    #currentPrice = iter[2]
                    if currentDate != 'unknown' and currentDate > checkPrice:
                        priceResults.append(encodedID)
               #     iter = reviewsCursor.next()
                    #checkPrice = priceTerm[2]
        ppriceIndex+=1

'''
if termLengthTable[0] < 0:
    if termLengthTable[1] < 2:#no range search
        if (len(validLowerPriceIDs) > len(validHigherPriceIDs)):
            priceResults = validLowerPriceIDs
        else:
            priceResults = validHigherPriceIDs
    else:
        for ID in validLowerPriceIDs and validHigherPriceIDs:
            priceResults.append(ID)
else:
    priceResults = dateResults
'''
if termLengthTable[0] > 0:
    priceResults = set(priceResults)
    priceResults = priceResults & dateResults
else:
    priceResults = dateResults
print("price",priceResults)
#if len(parsedSearch[])
    #output
    #encodedDate = datetime.datetime.fromtimestamp(int(iter[7])).strftime("%Y/%m/%d") # from timestamp format
    #print(encodedDate)
    #print(iter[7])
    #encodeDate = datetime.datetime.fromtimestamp(iter[7]).strftime("%Y/%m/%d") # from timestamp format
    #print(encodeDate)

    #iter = reviewsCursor.next()

    #while iter:
    #    print(iter)
    #    iter = reviewsCursor.next()
    #print(iter)


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
    a=iter[0]
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
