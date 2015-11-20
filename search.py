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


search = input("input querey u fuk: ")
#return (pterms,rterms,pprice,rscore,rdate,part_terms,terms)
parsedSearch = queryData(search)
searchIndex = 0
while searchIndex < len(parsedSearch)+1:
    if len(parsedSearch[searchIndex])!=0:
        for termsIndex in range(0, len(parsedSearch[searchIndex])):
            if searchIndex == 0:
                print(parsedSearch[searchIndex[termsIndex]])
                #print(pterms.get(parsedSearch[searchIndex[termsIndex]].encode(),end="\n\n"))


                break
            break
        break
    break

print(pterms.get(b'clothing').decode(), end="\n\n")



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
