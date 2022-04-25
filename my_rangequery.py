import heapq

PRIME = 2
#Using Rolling hash
def hash_string(id):
    num = 0
    for i in id:
        num = (num*PRIME + ord(i))%PRIME
    return num

def RangeQuery(ratingMinValue, ratingMaxValue, keylength, openconnection, openconnectionAWS):
    data = []
    cur = openconnection.cursor()
    cursorAWS = []
    for i in range(2):
        cursorAWS.append(openconnectionAWS[i].cursor())
    cur.execute('select current_database()')

    db_name = cur.fetchall()[0][0]
    cur.execute('select PartitionNum from RangeRatingsMetadata')
    range_partitions = cur.fetchall()

    for pno in range_partitions:
        table_name = "RangeRatingsPart" + str(pno[0]-1)
        cursorAWS[pno[0]-1].execute("select userid, rating from " + table_name + " where userid >= '" + ratingMinValue + "' and userid <= '" + ratingMaxValue + "'")
        matches = cursorAWS[pno[0]-1].fetchall()

        for match in matches:
            partition = "RangeRatingsPart" + table_name[-1]
            data.append(str(partition) + "," + str(match[0]) + "," + str(match[1]))
    
    cur.close()
    for i in range(2):
        if (cursorAWS[i]):
            cursorAWS[i].close()

    fh = open("RangeQueryOutnormal.txt","w")
    fh.write("\n".join(data))
    fh.close()



