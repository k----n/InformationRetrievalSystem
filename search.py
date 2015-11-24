from query_parser import queryData
import datetime
from bsddb3 import db
from csv import reader

def termSearch(parsedSearch, ptermsCursor, rtermsCursor):
    termLengthTable = []
    # make a list to check the number of terms in the parsedSearch
    termLengthTable.append(len(parsedSearch[0]))    # length of pterms
    termLengthTable.append(len(parsedSearch[1]))    # length of rterms
    termLengthTable.append(len(parsedSearch[5]))    # length of part_terms
    termLengthTable.append(len(parsedSearch[6]))    # length of terms
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
    for results in completeList:
        if len(results) > 0:
            resultIDs = resultIDs & set(results)
    return resultIDs

def numberFilter(parsedSearch, resultIDs, lowerScoresCursor, higherScoresCursor, reviewsCursor, reviews):
    termLengthTable = list()
    termLengthTable.append(len(parsedSearch[2]))    # length of pprice
    termLengthTable.append(len(parsedSearch[3]))    # length of rscore
    termLengthTable.append(len(parsedSearch[4]))    # length of rdate
    #get all the valid ids and merge both lists if there are any
    index = 0
    validLowerScoreIDs = list()
    validHigherScoreIDs = list()
    for amount in range(0, termLengthTable[1]):
        signChecked = parsedSearch[3][index][1]
        numberChecked = float(parsedSearch[3][index][2])
        if signChecked == "<":    #less than search. gets all the items from beginning up to the number
            lowerIter = lowerScoresCursor.first()

            while lowerIter is not None and float(lowerIter[0].decode()) < numberChecked:
                validLowerScoreIDs.append(lowerIter[1])
                lowerIter = lowerScoresCursor.next()
        elif signChecked == ">":  #greater than search. gets all the items from end down to the number
            upperIter = higherScoresCursor.last()
            while upperIter is not None and float(upperIter[0].decode()) > numberChecked:
                validHigherScoreIDs.append(upperIter[1])
                upperIter=higherScoresCursor.prev()
        index+=1

    scoreResults = resultIDs
    if termLengthTable[1] > 1:
        scoreResults = scoreResults & validLowerScoreIDs
        scoreResults = scoreResults & validHigherScoreIDs
    elif termLengthTable[1] == 1:
        if signChecked == '<':
            scoreResults = scoreResults & set(validLowerScoreIDs)
        elif signChecked == '>':
            scoreResults = scoreResults & set(validHigherScoreIDs)

    '''
    # datetime conversion
    datetime.datetime.strptime("2013/02/14","%Y/%m/%d").timestamp() #to timestamp format
    datetime.datetime.fromtimestamp(int(1369029600)).strftime("%Y/%m/%d") # from timestamp format
    '''
    dateIndex = 0 # dateIndex is terms in parsed search under rdate, 0 = no dates, 1 = 1 date, 2 = range of dates
    validLowerDateIDs = list()
    validHigherDateIDs = list()
    dateResults = list()
    if (termLengthTable[2]) > 0:   #there is some rdate criterias
        while dateIndex < termLengthTable[2]:
            for encodedID in scoreResults:
                iter = reviewsCursor.first()
                iter = reviews.get(encodedID, db.DB_SET)
                iter = iter.decode()
                iter = [entry for entry in reader([iter])][0]
                dateTerm = parsedSearch[4][dateIndex]
                extractedDate = str(datetime.datetime.strptime(parsedSearch[4][dateIndex][2],"%Y/%m/%d").timestamp())  #date of input
                currentDate = iter[7]   #date of data
                if dateTerm[1] == '<':
                        if float(currentDate) < float(extractedDate):
                            validLowerDateIDs.append(encodedID)
                if dateTerm[1] == '>':
                        if float(currentDate) > float(extractedDate):
                            validHigherDateIDs.append(encodedID)
            dateIndex += 1
    dateResults = scoreResults
    if termLengthTable[2] > 1:
        dateResults = dateResults & set(validLowerDateIDs)
        dateResults = dateResults & set(validHigherDateIDs)
    elif termLengthTable[2] == 1:
        if parsedSearch[4][0][1] == '<':
            dateResults = dateResults & set(validLowerDateIDs)
        elif parsedSearch[4][0][1] == '>':
            dateResults = dateResults & set(validHigherDateIDs)

    ppriceIndex = 0
    validLowerPriceIDs = list()
    validHigherPriceIDs = list()
    priceResults = list()
    if (termLengthTable[0])>0: #there are some pprice criteria
        while ppriceIndex < termLengthTable[0]:
            for encodedID in dateResults:
                iter = reviews.get(encodedID, db.DB_SET)
                iter = iter.decode()
                iter = [entry for entry in reader([iter])][0]
                priceTerm = parsedSearch[2][ppriceIndex]
                checkPrice = priceTerm[2]
                currentPrice = iter[2]
                if priceTerm[1] == '<':
                        if currentPrice != 'unknown' and float(currentPrice) < float(checkPrice):
                            validLowerPriceIDs.append(encodedID)
                if priceTerm[1] == '>':
                    if currentPrice != 'unknown' and float(currentPrice) > float(checkPrice):
                        validHigherPriceIDs.append(encodedID)
            ppriceIndex+=1

    priceResults = dateResults
    if termLengthTable[0] > 1:
        priceResults = priceResults & set(validLowerPriceIDs)
        priceResults = priceResults & set(validHigherPriceIDs)
    elif termLengthTable[0] == 1:
        if parsedSearch[2][0][1] == '<':
            priceResults = priceResults & set(validLowerPriceIDs)
        elif parsedSearch[2][0][1] == '>':
            priceResults = priceResults & set(validHigherPriceIDs)
    finalResults = priceResults
    return finalResults

def printReviews(finalResults, reviews):
    for ID in finalResults:
        review = reviews.get(ID, db.DB_SET)
        review = review.decode()
        review = [entry for entry in reader([review])][0]
        print("Entry:" + ID.decode())
        print("Product ID:" + review[0])
        print("Product Title:" + review[1])
        print("Product Price:" + review[2])
        print("UserID:" + review[3])
        print("Profile Name:" + review[4])
        print("Helpfulness:" + review[5])
        print("Score:" + review[6])
        print("Review Time:" + datetime.datetime.fromtimestamp(int(review[7])).strftime("%Y/%m/%d"))
        print("Review Summary:" + review[8])
        print("Review Text:" + review[9])
        print("------------")

def returnResults(query):
    #Initialization of index files using BerkeleyDB
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

    parsedSearch = queryData(query)
    terms = termSearch(parsedSearch, ptermsCursor, rtermsCursor)
    filtered = numberFilter(parsedSearch, terms, lowerScoresCursor, higherScoresCursor, reviewsCursor, reviews)
    printReviews(filtered, reviews)

    reviewsCursor.close()
    ptermsCursor.close()
    rtermsCursor.close()
    lowerScoresCursor.close()
    higherScoresCursor.close()
    reviews.close()
    pterms.close()
    rterms.close()
    scores.close()

if __name__ == "__main__":
    print("Enter nothing to exit")
    while True:
        query = input("query: ")
        if query == "":
            break

        returnResults(query)

