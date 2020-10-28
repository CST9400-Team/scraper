from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import csv
import json

import pymongo

listingPage = urlopen(Request("https://www.imdb.com/chart/top", headers={'User-Agent': 'Mozilla/5.0'})).read()
primarySoup = BeautifulSoup(listingPage, 'html.parser')

counter = 0
arr = []
tdTags = primarySoup.find(["tbody",{"class":"lister-list"}])
for title in tdTags.findAll(["tr"]):
    try:
        counter+=1
        arr.append(title.find(["div",{"class":"wlb_ribbon"}])["data-titleid"])
    except Exception:
        print("Exception occured")

'''
with open('data.csv', 'w',newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    for i in arr:
        writer.writerow([i])
'''


client = pymongo.MongoClient("mongodb+srv://tejus:team20@cluster0.ymzum.gcp.mongodb.net/testDB?retryWrites=true&w=majority")
db = client.test
mycol = db["testCollection"]

for tID in arr:
    omdbPage = urlopen(Request("http://www.omdbapi.com/?apikey=8fa02782&i="+tID, headers={'User-Agent': 'Mozilla/5.0'})).read()
    omdbJson = json.loads(omdbPage)
    title = omdbJson["Title"]
    year = omdbJson["Year"]
    rated = omdbJson["Rated"]
    released = omdbJson["Released"]
    runtime = omdbJson["Runtime"]
    genre = omdbJson["Genre"]
    runtime = omdbJson["Director"]
    
    x = mycol.insert_one(omdbJson)

    