#-------------------------------------------------------------------------
# AUTHOR: Christopher Ernesto
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Creating an inverted index using python and MongoDB
# FOR: CS 4250- Assignment #3
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient
import datetime

def connectDataBase():

    # Create a database connection object using pymongo

    DB_NAME = "corpus"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")


def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.

    # lowercase text
    parseText = docText.lower()

    # remove punctuation
    punc = ".!:;,?"
    for ele in punc:
        if ele in parseText:
            parseText = parseText.replace(ele, "")

    # get terms
    terms = parseText.split()

    termDict = {}
    for word in terms:
        if word in termDict:
            termDict[word] = termDict[word] + 1
        else:
            termDict[word] = 1

    # print(termDict)

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    termArray = []
    for ele in termDict:
        num_char = len(ele)
        tempObj = {"term": ele, "term_count": termDict[ele], "num_chars": num_char}
        termArray.append(tempObj)

    # print(termArray)

    # produce a final document as a dictionary including all the required document fields
    doc_num_char = len(parseText.replace(" ",""))
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "num_chars": doc_num_char,
        "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "category": docCat,
        "terms": termArray
    }

    # insert the document
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    pipeline = [
        {"$unwind": {"path": "$terms"}},
        {"$sort": {"terms.term": 1}}
    ]

    documents = col.aggregate(pipeline)

    index = {}

    for doc in documents:
        docs = "%s:%s" % (doc["title"], doc["terms"]["term_count"])
        if doc["terms"]["term"] in index:
            val = index[doc["terms"]["term"]] + ", " + docs
            index[doc["terms"]["term"]] = val
        else:
            index[doc["terms"]["term"]] = docs

    return index
