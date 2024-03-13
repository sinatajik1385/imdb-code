import pandas as pd
import numpy as np
import os
class from_tsv_to_sql ():
    def titleEpisode_cleanup() :
        directory = f"{directo}/cleaned_up"
        if os.path.isfile(f"{directory}/titleEpisode.csv") == True :
            print ("file already exists")
        else:
            header = 1
            df = pd.read_csv ("titleEpisode.tsv",chunksize=15000, sep= "\t")
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
                if header == 1 :
                    header += 1 
                    final_dataframe = pd.DataFrame.from_dict(dictionary_seasonnumber_episodenumber)
                    final_dataframe.to_csv(f"{directory}/titleEpisode.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_seasonnumber_episodenumber)
                    final_dataframe.to_csv(f"{directory}/titleEpisode.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
    def titleAkas_cleanup() :
        directory = f"{directo}/cleaned_up"
        if os.path.isfile(f"{directory}/titleAkas.csv") == True :
            print ("file already exists")
        else :
            header = 1
            index_pk = 1
            df = pd.read_csv ("titleAkas.tsv", chunksize=15000 ,sep= "\t")
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
                if header == 1 :
                    header +=1 
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/titleAkas.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/titleAkas.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
    def titleBasics_cleanup() :
        directory = f"{directo}/cleaned_up"
        if os.path.isfile(f"{directory}/titleBasics.csv") == True :
            print ("file already exists")
        else :     
            df = pd.read_csv ("titleBasics.tsv", chunksize=15000 ,sep= "\t")
            index_pk = 1
            header = 1
            for chunks in df :
                dictionary_dict_isadult_runtime = []
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
                if header == 1 :
                    header += 1                     
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_isadult_runtime)
                    final_dataframe.to_csv(f"{directory}/titleBasics.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_isadult_runtime)
                    final_dataframe.to_csv(f"{directory}/titleBasics.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
    def titleCrew_cleanup() :
        directory = f"{directo}/cleaned_up"
        if os.path.isfile(f"{directory}/titleCrew.csv") == True :
            print ("file already exists")
        else :
            index_pk = 1
            header = 1
            df = pd.read_csv ("titleCrew.tsv", chunksize=15000 ,sep= "\t")
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
                if header == 1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_directors_writers)
                    final_dataframe.to_csv(f"{directory}/titleCrew.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_directors_writers)
                    final_dataframe.to_csv(f"{directory}/titleCrew.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
    def titleRatings_cleanup() :
        directory = f"{directo}/cleaned_up"
        if os.path.isfile(f"{directory}/titleRatings.csv") == True :
            print ("file already exists")
        else :
            header = 1 
            df = pd.read_csv ("titleRatings.tsv", chunksize=15000 ,sep= "\t")
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
                if header ==1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_averagerating_numberofvotes)
                    final_dataframe.to_csv(f"{directory}/titleRatings.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_averagerating_numberofvotes)
                    final_dataframe.to_csv(f"{directory}/titleRatings.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
    def nameBasics_cleanup() :
        directory = f"{directo}/cleaned_up"
        if os.path.isfile(f"{directory}/nameBasics.csv") == True :
            print ("file already exists")
        else :
            df = pd.read_csv ("nameBasics.tsv", chunksize=15000 ,sep= "\t")
            header = 1
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
                if header == 1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
                    final_dataframe.to_csv(f"{directory}/nameBasics.csv",mode="a",header=True,encoding='utf-8-sig',index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
                    final_dataframe.to_csv(f"{directory}/nameBasics.csv",mode="a",header=False,encoding='utf-8-sig',index=False)
    def titlePrincipals_cleanup() :
        directory = f"{directo}/cleaned_up"
        if os.path.isfile(f"{directory}/titlePrincipals.csv") == True :
            print ("file already exists")
        else :
            header = 1 
            df = pd.read_csv ("titlePrincipals.tsv", chunksize=150000 ,low_memory=False,sep= "\t")
            for chunk in df : 
                if header == 1 :
                    header += 1      
                    chunk.to_csv(f"{directory}/titlePrincipals.csv",mode ="a",header=True,encoding='utf-8-sig' , index=True)
                else :
                    chunk.to_csv(f"{directory}/titlePrincipals.csv",mode ="a",header=False,encoding='utf-8-sig' , index=True)
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
directo = os.getcwd()
try :
    os.mkdir(f"{directo}/cleaned_up")
except :
    pass 
finally :
    while True : 
        menu = input(f"----------\n1.cleanup the data\n-----------\nplease enter : ")
        if menu == "1" :
            print (f"starting \n ----------------------------- ")
            from_tsv_to_sql.titleEpisode_cleanup()
            print (f"1 done \n ----------------------------- ")
            from_tsv_to_sql.titleAkas_cleanup()
            print (f"2 done \n ----------------------------- ")
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
