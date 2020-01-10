import os
import sys
import lib.keys_and_secrets as ks
from googleapiclient.discovery import build   #Import the library
import scrapper_v2 as sc2


def get_legit_names(names):
    legit = []
    for name in names:
        names_split = name.split(" ")
        if len(names_split)>1 and len([name for name in names_split if len(name)>2])>1:
            legit.append(name)
    return legit



def google_query(query, api_key, cse_id, **kwargs):
    query_service = build("customsearch", 
                          "v1", 
                          developerKey=api_key
                          )  
    query_results = query_service.cse().list(q=query,    # Query
                                             cx=cse_id,  # CSE ID
                                             **kwargs    
                                             ).execute()
    return query_results['items']

def main():
    with open(os.path.join(sys.path[0], "names.txt"), "r") as f:
        names = [x.replace("\n","") for x in f.readlines()]
    legit_names = get_legit_names(names)
    api_key = ks.api_key
    cse_id = ks.cse_id
    my_results = []
    with open("results.txt", "w+") as f:
        f.write("name;Google Search title; LinkedIn Profile Link")
        for name in legit_names:
            title, link = sc2.get_results(name + " site:linkedin.com/in OR site:linkedin.com/pub -pub.dir")
            print(name, title, link)
            f.write(name + ";" + title + ";" + link)
            f.write("\n")
            """result = google_query(f"linkedin.com {name}",
                                    api_key, 
                                    cse_id, 
                                    num = 1
                                    )[0]

            person = {"title" : result['title'], "link": result["link"], "name": name}
            """
    print (my_results)

if __name__ == '__main__':
    main()