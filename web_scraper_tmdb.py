from bs4 import BeautifulSoup
import requests
import os
import pandas as pd

directo = os.getcwd()
directory = f"{directo}/cleaned_up_nf2"
if os.path.isfile(f"{directory}/tmdb_movies_budget_revenue.csv") == True :
    print ("file already exists")
else :     
    df = pd.read_csv ("titleBasics.tsv", chunksize=50000 ,sep= "\t")
    header = 1
    for chunks in df :
        dictionary_dict_tmdb = []
        chunks.drop_duplicates()
        data_dict_tmdb = chunks.to_dict(orient= "records")
        for i in data_dict_tmdb :
            remade_dictionary = {}
            tconst = i["tconst"]
            if i["originalTitle"] == 0 :
                pass
            else :
                header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
                html_text = requests.get (f"https://www.themoviedb.org/search?query="+i["originalTitle"]+"%20y%3A"+i["startYear"] , headers=header).text
                soup = BeautifulSoup(html_text , "html.parser")
                container = soup.find("a" , class_= "result")
                url_result = str(container).split('"')
                for i in range (len(url_result)) :
                    i =int(i)
                    if " href=" in url_result[i] :
                        header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'} 
                        html_text1 = requests.get ("https://www.themoviedb.org/"+url_result[i+1] , headers= header).text
                        soup1 = BeautifulSoup (html_text1 , "html.parser")
                        try :
                            movie_stats = soup1.find ("section" , class_="facts left_column").text
                            movie_stats1 = str(movie_stats).split("\n")
                            for x in range (len(movie_stats1)) :
                                x = int (x)
                                movie_stats_budget = str(movie_stats1[-2]).split(" ")
                                movie_stats_revenue = str(movie_stats1 [-3]).split(" ")
                                print (movie_stats_budget ,movie_stats_revenue )
                                print("web scraped")
                                remade_dictionary = {"tconst" : f"{tconst}" , "Budget" : f"{movie_stats_budget[-1]}" , "Revenue" : f"{movie_stats_revenue[-1]}"  }
                                dictionary_dict_tmdb.append(remade_dictionary)
                        except :
                            pass
        if header == 1 :
            header += 1                     
            final_dataframe = pd.DataFrame.from_dict(dictionary_dict_tmdb)
            final_dataframe.to_csv(f"{directory}/tmdb_movies_budget_revenue.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
        else :
            final_dataframe = pd.DataFrame.from_dict(dictionary_dict_tmdb)
            final_dataframe.to_csv(f"{directory}/tmdb_movies_budget_revenue.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)   
