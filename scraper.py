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
db = client["testDB"]
mycol = db["testCollection"]


for tID in arr:
    omdbPage = urlopen(Request("http://www.omdbapi.com/?apikey=8fa02782&i="+tID, headers={'User-Agent': 'Mozilla/5.0'})).read()
    omdbJson = json.loads(omdbPage)
    title = omdbJson["Title"]
    year = int(omdbJson["Year"])
    rated = omdbJson["Rated"]
    released = omdbJson["Released"]
    runtime = omdbJson["Runtime"]
    runtime = int(runtime[0:-4])

    genres = []
    for g in omdbJson["Genre"].split(", "):
        genres.append(g)
    genre = genres

    directors = []
    for d in omdbJson["Director"].split(", "):
        directors.append(d)
    director = directors

    writer = omdbJson["Writer"]

    actors = []
    for a in omdbJson["Actors"].split(", "):
        actors.append(a)
    actor = actors
    
    plot = omdbJson["Plot"]

    languages = []
    for l in omdbJson["Language"].split(", "):
        languages.append(l)
    language = languages

    countries = []
    for c in omdbJson["Country"].split(", "):
        countries.append(c)
    country = countries

    plot = omdbJson["Plot"]
    awards = omdbJson["Awards"]
    poster = omdbJson["Poster"]
    ratings = omdbJson["Ratings"]
    metascore = omdbJson["Metascore"]
    imdbRating = omdbJson["imdbRating"]
    imdbVotes = omdbJson["imdbVotes"]
    imdbID = omdbJson["imdbID"]
    typeOfMovie = omdbJson["Type"]
    dvd = omdbJson["DVD"]

    productions = []
    for p in omdbJson["Production"].split(", "):
        productions.append(p)
    production = productions

    dictionary = {
        "Title": title,
        "Year": year,
        "Rated":rated,
        "Released":released,
        "Runtime": runtime,
        "Genre":genres,
        "Directors":directors,
        "Writer":writer,
        "Actors": actor,
        "Plot": plot,
        "Language": language,
        "Country": country,
        "Awards": awards,
        "Poster":poster,
        "Ratings": ratings,
        "Metascore": metascore,
        "imdbRating": imdbRating,
        "imdbVotes": imdbVotes,
        "imdbID": imdbID,
        "Type": typeOfMovie,
        "DVD": dvd,
        "Productions": production
    }
    print(dictionary)
    x = mycol.insert_one(dictionary)

    