import pandas as pd
import numpy as np
class from_tsv_to_sql ():
    def titleEpisode_cleanup() :
        df = pd.read_csv ("titleEpisode.tsv",chunksize=50000, sep= "\t")
        for chunks in df :
            dictionary_seasonnumber_episodenumber = []
            chunks.drop_duplicates()
            data_dict_seasonnumber_episodenumber = chunks.to_dict(orient = "records")
            for i in data_dict_seasonnumber_episodenumber :
                if i["seasonNumber"] == "\\N" :
                    pass
                elif i["episodeNumber"] == "\\N" :
                    pass
                else :
                    dictionary_seasonnumber_episodenumber.append(i)
            final_dataframe = pd.DataFrame.from_dict(dictionary_seasonnumber_episodenumber)
            final_dataframe.to_csv("F:/taklif/cleaned_up/titleEpisode.csv",mode="a",header="False",encoding='utf-8-sig' , index=True)
    def titleAkas_cleanup() :
        index_pk = 1
        df = pd.read_csv ("titleAkas.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df :
            dictionary_dict_title_and_labguages = []
            chunks.drop_duplicates()
            data_dict_title_and_labguages = chunks.to_dict(orient= "records")
            for i in data_dict_title_and_labguages :
                if i["region"] == "\\N" :
                        pass
                elif i["language"] == "\\N" :
                    pass
                else :
                    titleId =  i["titleId"]
                    ordering = i["ordering"]
                    title = i["title"]
                    region = i["region"]
                    language = i["language"]
                    types = i["types"]
                    attributes = i["attributes"]
                    isOriginalTitle = i["isOriginalTitle"]
                    remade_dictionary = { "index_pk" : f"{index_pk}" ,"titleId" : f"{titleId}" , "ordering" : f"{ordering}" , "title" : f"{title}" , "region" : f"{region}" , "language" : F"{language}" , "types" : f"{types}" , "attributes" : f"{attributes}" , "isOriginalTitle" : f"{isOriginalTitle}" }
                    dictionary_dict_title_and_labguages.append(remade_dictionary)
                    index_pk += 1 
            final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
            final_dataframe.to_csv("F:/taklif/cleaned_up/titleAkas.csv",mode="a",header="False",encoding='utf-8-sig' , index=False)
    def titleBasics_cleanup() :     
        df = pd.read_csv ("titleBasics.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df :
            dictionary_dict_isadult_runtime = []
            index_pk =1
            chunks.drop_duplicates()
            data_dict_isadult_runtime = chunks.to_dict(orient= "records")
            for i in data_dict_isadult_runtime :
                if i["runtimeMinutes"] == "\\N" :
                    pass
                else :
                    number_of_genres = str(i["genres"]).split(",")
                    for data_number_of_genres in number_of_genres :
                        tconst = i["tconst"]
                        titleType = i["titleType"]
                        primaryTitle = i["primaryTitle"]
                        originalTitle = i["originalTitle"]
                        isAdult = i["isAdult"]
                        startYear = i["startYear"]
                        endYear = i["endYear"]
                        runtimeMinutes = i["runtimeMinutes"]
                        genres = data_number_of_genres
                        remade_dictionary = { "index_pk" : f"{index_pk}" ,"tconst" : f"{tconst}" , "titleType" : f"{titleType}" , "primaryTitle" : f"{primaryTitle}" , "originalTitle" : f"{originalTitle}" , "isAdult" : F"{isAdult}" , "startYear" : f"{startYear}" , "endYear" : f"{endYear}" , "runtimeMinutes" : f"{runtimeMinutes}" , "genres" : f"{genres}"}
                        dictionary_dict_isadult_runtime.append(remade_dictionary)
                        index_pk += 1                     
            final_dataframe = pd.DataFrame.from_dict(dictionary_dict_isadult_runtime)
            final_dataframe.to_csv("F:/taklif/cleaned_up/titleBasics.csv",mode="a",header="False",encoding='utf-8-sig' , index=False)
    def titleCrew_cleanup() :
        index_pk = 1
        df = pd.read_csv ("titleCrew.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df :
            dictionary_dict_directors_writers = []
            chunks.drop_duplicates()
            data_dict_directors_writers = chunks.to_dict(orient= "records")
            for i in data_dict_directors_writers :
                if i["directors"] == "\\N" :
                        pass
                else :
                    number_of_directors =  i["directors"].split(",")
                    number_of_writers = i["writers"].split(",")
                    for data_number_of_directors in number_of_directors   :
                            for data_number_of_writers in number_of_writers :
                                tconst = i["tconst"]
                                directors = data_number_of_directors
                                writers = data_number_of_writers
                                remade_dictionary = { "index_pk" : f"{index_pk}" ,"tconst" : f"{tconst}" , "directors" : f"{directors}" , "writers" : f"{writers}"}
                                dictionary_dict_directors_writers.append(remade_dictionary)
                                index_pk += 1 
            final_dataframe = pd.DataFrame.from_dict(dictionary_dict_directors_writers)
            final_dataframe.to_csv("F:/taklif/cleaned_up/titleCrew.csv",mode="a",header="False",encoding='utf-8-sig' , index=False)
    def titleRatings_cleanup() :
        df = pd.read_csv ("titleRatings.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df :
            dictionary_dict_averagerating_numberofvotes = []
            chunks.drop_duplicates()
            data_dict_averagerating_numberofvotes = chunks.to_dict(orient= "records")
            for i in data_dict_averagerating_numberofvotes :
                if i["averageRating"] == "\\N" :
                        pass
                elif i["numVotes"] == "\\N" :
                    pass
                else :
                    dictionary_dict_averagerating_numberofvotes.append(i)
            final_dataframe = pd.DataFrame.from_dict(dictionary_dict_averagerating_numberofvotes)
            final_dataframe.to_csv("F:/taklif/cleaned_up/titleRatings.csv",mode="a",header="False",encoding='utf-8-sig' , index=False)
    def nameBasics_cleanup() :
        df = pd.read_csv ("nameBasics.tsv", chunksize=50000 ,sep= "\t")
        index_pk = 1
        for chunks in df :
            dictionary_dict_actornames = []
            chunks.drop_duplicates()
            data_dict_actornames = chunks.to_dict(orient= "records")
            for i in data_dict_actornames :
                if i["birthYear"] == "\\N" :
                        pass
                elif i["knownForTitles"] == "\\N" :
                    pass
                else :
                    number_of_titles = i["knownForTitles"].split(",")
                    number_of_proffesions = str(i["primaryProfession"]).split(",")
                    for data_number_of_proffesions in number_of_proffesions:
                        remade_dictionary = {}                        
                        for data_number_of_titles in  number_of_titles :
                            nconst = i["nconst"]
                            primaryName = i["primaryName"] 
                            birthYear = i["birthYear"]
                            deathYear = i["deathYear"]
                            primaryProfession = data_number_of_proffesions
                            knownForTitles = data_number_of_titles
                            remade_dictionary = { "index_pk" : f"{index_pk}" ,"nconst" : f"{nconst}" , "primaryName" : f"{primaryName}" , "birthYear" : f"{birthYear}" , "deathYear" : f"{deathYear}" , "primaryProfession" : f"{primaryProfession}" , "knownForTitles" : f"{knownForTitles}"}
                            dictionary_dict_actornames.append(remade_dictionary)
                            index_pk += 1
            final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
            final_dataframe.to_csv("F:/taklif/cleaned_up/nameBasics.csv",mode="a",header="False",encoding='utf-8-sig',index=False)
    def titlePrincipals_cleanup() :
        df = pd.read_csv ("titlePrincipals.tsv", chunksize=500000 ,low_memory=False,sep= "\t")
        for chunk in df :       
            chunk.to_csv("F:/taklif/cleaned_up/titlePrincipals.csv",mode ="a",header="False",encoding='utf-8-sig' , index=True)
from_tsv_to_sql.titleAkas_cleanup()
"""⠛⠋⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⠉⣻⣩⣉⠉⠉
⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⢀⣀⣀⣀⣀⣀⣀⡀⠄⠄⠉⠉⠄⠄⠄
⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⣠⣶⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣤⠄⠄⠄⠄
⠄⠄⠄⠄⠄⠄⠄⠄⠄⢤⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡀⠄⠄⠄
⡄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠉⠄⠉⠉⠉⣋⠉⠉⠉⠉⠉⠉⠉⠉⠙⠛⢷⡀⠄⠄
⣿⡄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠠⣾⣿⣷⣄⣀⣀⣀⣠⣄⣢⣤⣤⣾⣿⡀⠄
⣿⠃⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⣹⣿⣿⡿⠿⣿⣿⣿⣿⣿⣿⣿⣿⢟⢁⣠
⣿⣿⣄⣀⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠉⠉⣉⣉⣰⣿⣿⣿⣿⣷⣥⡀⠉⢁⡥⠈
⣿⣿⣿⢹⣇⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠒⠛⠛⠋⠉⠉⠛⢻⣿⣿⣷⢀⡭⣤⠄
⣿⣿⣿⡼⣿⠷⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⢀⣀⣠⣿⣟⢷⢾⣊⠄⠄
⠉⠉⠁⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠈⣈⣉⣭⣽⡿⠟⢉⢴⣿⡇⣺⣿⣷
⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠄⠁⠐⢊⣡⣴⣾⣥"""
while True : 
    menu = input(f"----------\n1.cleanup the data\n-----------\nplease enter : ")
    if menu == "1" :
        print (f"starting \n ----------------------------- ")
        from_tsv_to_sql.titleEpisode_cleanup()
        print (f"1 done \n ----------------------------- ")
#        from_tsv_to_sql.titleAkas_cleanup()
#        print (f"2 done \n ----------------------------- ")
### doesnt work :(((
        from_tsv_to_sql.titleBasics_cleanup()
        print (f"3 done \n ----------------------------- ")
        from_tsv_to_sql.titleCrew_cleanup()
        print (f"4 done \n ----------------------------- ")
        from_tsv_to_sql.titleRatings_cleanup()
        print (f"5 done \n ----------------------------- ")
        from_tsv_to_sql.nameBasics_cleanup()
        print (f"6 done \n ----------------------------- ")
        from_tsv_to_sql.titlePrincipals_cleanup()
        print (f"all done \n ----------------------------- ")
    else :
        print ("option not available ")
