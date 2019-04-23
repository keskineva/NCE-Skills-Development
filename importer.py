# 1. load the file
import csv
import sys
import re
import json
import datetime
import ast
import time
import requests
from enum import IntEnum


class CsvRecord(IntEnum):
    Hotel_Address = 0
    Additional_Number_of_Scoring = 1
    Review_Date = 2
    Average_Score = 3
    Hotel_Name = 4
    Reviewer_Nationality = 5
    Negative_Review = 6
    Review_Total_Negative_Word_Counts = 7
    Total_Number_of_Reviews = 8
    Positive_Review = 9
    Review_Total_Positive_Word_Counts = 10
    Total_Number_of_Reviews_Reviewer_Has_Given = 11
    Reviewer_Score = 12
    Tags = 13
    days_since_review = 14
    lat = 15
    lng = 16


print("Processing file: " + sys.argv[1])


def float_try_parse(value):
    try:
        return float(value), True
    except ValueError:
        return value, False


with open(sys.argv[1]) as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    documentsArray = {}
    # for i in range(1, readCSV.size):
    iterreadCSV = iter(readCSV)
    next(iterreadCSV)
    bulkRequests = []

    # cache map for addresses
    geoCacheDictionary = {}

    for csv in iterreadCSV:

        # construct the json file
        tagsClean = list(map(str.strip, ast.literal_eval(csv[CsvRecord.Tags])))
        location = None
        resultLon = float_try_parse(csv[CsvRecord.lng])
        resultLat = float_try_parse(csv[CsvRecord.lat])
        country = None
        city = None
        if resultLon[1] and resultLat[1]:
            location = {
                    "lon": resultLon[0],
                    "lat": resultLat[0]
                }
            # do reverse geocoding here by calling an API for that
            *first, last1, last2, last3 = csv[CsvRecord.Hotel_Address].strip().split()
            locationKey = last1 + last2 + last3  # the caching key is the last 3 words of the address
            if locationKey in geoCacheDictionary:
                city = geoCacheDictionary[locationKey]["city"]
                country = geoCacheDictionary[locationKey]["country"]
            else:
                # https://locationiq.com/v1/reverse_sandbox.php?format=json&lat=51.5195688&lon=-0.170521&accept-language=en
                print("Requesting GEO service for %s:%s..." % (location["lat"], location["lon"]))
                time.sleep(1)  # there is a quote 2 queries per second. So wait the for the cooldown period
                response = requests.get(
                    "https://eu1.locationiq.com/v1/reverse.php",
                    params={
                        "key": "94a9f62bc81ce4",
                        "format": "json",
                        "lat": location["lat"],
                        "lon": location["lon"]
                    }
                )

                if response:
                    geoInfoJson = json.loads(response.text)
                    if 'address' in geoInfoJson:
                        if "country" in geoInfoJson["address"]:
                            country = geoInfoJson["address"]["country"]

                        if "city" in geoInfoJson["address"]:
                            city = geoInfoJson["address"]["city"]
                        elif "town" in geoInfoJson["address"]:
                            city = geoInfoJson["address"]["town"]
                        elif "state" in geoInfoJson["address"]:
                            city = geoInfoJson["address"]["state"]
                        else:
                            print("Can't find city in " + json.dumps(geoInfoJson))

                        print("Revesed geocoding for %s, %s" % (city, country))
                        # caching...
                        geoCacheDictionary[locationKey] = {
                            "country": country,
                            "city": city
                        }
                    else:
                        print("Can't extract the geo coding... %s" % response.text)
                else:
                    print("Error to reverse geocoding... " + response.text)

        isWithPet = False
        tripType = None
        travelerType = None
        roomType = None
        stayedNights = None
        isSubmittedFromMobileDevice = False
        if len(tagsClean) > 0:
            tagIndex = 0
            if len(tagsClean) - 1 >= tagIndex and tagsClean[tagIndex].endswith("With a pet"):
                isWithPet = True
                tagIndex += 1
            if len(tagsClean) - 1 >= tagIndex and tagsClean[tagIndex].endswith(" trip"):
                tripType = tagsClean[tagIndex]
                tagIndex += 1
            if len(tagsClean) - 1 >= tagIndex:
                travelerType = tagsClean[tagIndex]
                tagIndex += 1
            if len(tagsClean) - 1 >= tagIndex:
                roomType = tagsClean[tagIndex]
                tagIndex += 1
            if len(tagsClean) - 1 >= tagIndex and tagsClean[tagIndex].startswith("Stayed "):
                numbers = re.findall(r'\d+', tagsClean[tagIndex])
                numbers = map(int, numbers)
                stayedNights = max(numbers)
                tagIndex += 1
            if len(tagsClean) - 1 >= tagIndex and tagsClean[tagIndex].startswith("Submitted from a mobile device"):
                isSubmittedFromMobileDevice = True
                tagIndex += 1

        positiveReviewClean = None
        if csv[CsvRecord.Positive_Review].strip().lower() != "no positive":
            positiveReviewClean = csv[CsvRecord.Positive_Review].strip()

        negativeReviewClean = None
        if csv[CsvRecord.Negative_Review].strip().lower() != "no negative" and \
                csv[CsvRecord.Negative_Review].strip().lower() != "nothing" and \
                csv[CsvRecord.Negative_Review].strip().lower() != "nothing at all":
            negativeReviewClean = csv[CsvRecord.Negative_Review].strip()

        oneDocument = {
            "Additional_Number_of_Scoring": int(csv[CsvRecord.Additional_Number_of_Scoring]),
            "Average_Score": float(csv[CsvRecord.Average_Score]),
            "Hotel_Address": csv[CsvRecord.Hotel_Address].strip(),
            "Hotel_Name": csv[CsvRecord.Hotel_Name].strip(),
            "Negative_Review": negativeReviewClean,
            "Positive_Review": positiveReviewClean,
            "Review_Date": datetime.datetime.strptime(csv[CsvRecord.Review_Date], '%m/%d/%Y').strftime('%Y-%m-%dT%H:%M:%SZ'),
            "Review_Total_Negative_Word_Counts": int(csv[CsvRecord.Review_Total_Negative_Word_Counts]),
            "Review_Total_Positive_Word_Counts": int(csv[CsvRecord.Review_Total_Positive_Word_Counts]),
            "Reviewer_Nationality": csv[CsvRecord.Reviewer_Nationality].strip(),
            "Reviewer_Score": float(csv[CsvRecord.Reviewer_Score]),
            "Tags": tagsClean,
            "Total_Number_of_Reviews": int(csv[CsvRecord.Total_Number_of_Reviews]),
            "Total_Number_of_Reviews_Reviewer_Has_Given": int(csv[CsvRecord.Total_Number_of_Reviews_Reviewer_Has_Given]),
            "days_since_review": re.search(r'\d+', csv[CsvRecord.days_since_review]).group(),
            "location": location,
            "isWithPet": isWithPet,  # With a pet
            "tripType": tripType,  # Leisure trip
            "travelerType": travelerType,  # Solo, Couple, Family, Group ...
            "roomType": roomType,  # Superior Queen Room
            "stayedNights": stayedNights,  # Stayed 3 nights
            "isSubmittedFromMobileDevice": isSubmittedFromMobileDevice,  # Submitted from a mobile device
            "hotelLocCountry": country,
            "hotelLocCity": city
        }

        bulkRequests.append("{\"index\":{}}\n")
        bulkRequests.append(json.dumps(oneDocument))
        bulkRequests.append("\n")

        # 3. send POST to elastic in batches
        if len(bulkRequests) == 6000:

            data = ''.join(bulkRequests)
            r = requests.post("http://localhost:9200/hotels5/_doc/_bulk",
                              data=data,
                              headers={'content-type': 'application/json'})
            print(r.status_code, r.reason)

            print(r.text[:300] + '...')
            bulkRequests = []

    data = ''.join(bulkRequests)
    r = requests.post("http://localhost:9200/hotels5/review/_bulk",
                      data=data,
                      headers={'content-type': 'application/json'})
    print(r.status_code, r.reason)

    print(r.text[:300] + '...')
