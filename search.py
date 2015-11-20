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
search = "pprice < 60 pprice > 30 clothing"

parsedSearch = queryData(search)
print(parsedSearch)
searchIndex = 0
while searchIndex < len(parsedSearch):
    if len(parsedSearch[searchIndex])>0:
        for termsIndex in range(0, len(parsedSearch[searchIndex])):
            if searchIndex == 0:
                encodedTerm = (parsedSearch[searchIndex][termsIndex]).encode()
                ptermsID = pterms.get(encodedTerm)

            if searchIndex == 1:
                encodedTerm = (parsedSearch[searchIndex][termsIndex]).encode()
                rtermsID = rterms.get(encodedTerm)

            if searchIndex == 2:
                length = 0
                termstermsIndex = len(parsedSearch[6][length])
                encodedTerm = (parsedSearch[6][termstermsIndex]).encode()

    searchIndex+=1




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
