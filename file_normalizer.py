import pandas as pd
import numpy as np
class from_tsv_to_sql ():
    def data_seasonnumber_episodenumber_cleanup() :
        dictionary_seasonnumber_episodenumber = []
        df = pd.read_csv ("data_seasonnumber_episodenumber.tsv",chunksize=50000, sep= "\t")
        for chunks in df :
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
        final_dataframe.to_csv("F:/taklif/cleaned_up/data_seasonnumber_episodenumber.csv" , index=False)
    def data_title_and_labguages_cleanup() :
        dictionary_dict_title_and_labguages = []
        df1 = pd.read_csv ("data_title_and_labguages.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df1 :
            chunks.drop_duplicates()
            data_dict_title_and_labguages = chunks.to_dict(orient= "records")
            for i in data_dict_title_and_labguages :
                if i["region"] == "\\N" :
                        pass
                elif i["language"] == "\\N" :
                    pass
                else :
                    dictionary_dict_title_and_labguages.append(i)
        final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
        final_dataframe.to_csv("F:/taklif/cleaned_up/data_title_and_labguages_cleanedup.csv" , index=False)
    def data_isadult_runtime_cleanup() :
        dictionary_dict_isadult_runtime = []
        df1 = pd.read_csv ("data_isadult_runtime.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df1 :
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
                        remade_dictionary = { "tconst" : f"{tconst}" , "titleType" : f"{titleType}" , "primaryTitle" : f"{primaryTitle}" , "originalTitle" : f"{originalTitle}" , "isAdult" : F"{isAdult}" , "startYear" : f"{startYear}" , "endYear" : f"{endYear}" , "runtimeMinutes" : f"{runtimeMinutes}" , "genres" : f"{genres}"}
                        dictionary_dict_isadult_runtime.append(remade_dictionary)                     
        final_dataframe = pd.DataFrame.from_dict(dictionary_dict_isadult_runtime)
        final_dataframe.to_csv("F:/taklif/cleaned_up/data_isadult_runtime_cleanedup.csv" , index=True)
    def data_directors_writers_cleanup() :
        dictionary_dict_directors_writers = []
        df1 = pd.read_csv ("data_directors_writers.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df1 :
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
                                remade_dictionary = { "tconst" : f"{tconst}" , "directors" : f"{directors}" , "writers" : f"{writers}"}
                                dictionary_dict_directors_writers.append(remade_dictionary)
        final_dataframe = pd.DataFrame.from_dict(dictionary_dict_directors_writers)
        final_dataframe.to_csv("F:/taklif/cleaned_up/data_directors_writers_cleanedup.csv" , index=True)
    def data_averagerating_numberofvotes_cleanup() :
        dictionary_dict_averagerating_numberofvotes = []
        df1 = pd.read_csv ("data_averagerating_numberofvotes.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df1 :
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
        final_dataframe.to_csv("F:/taklif/cleaned_up/data_averagerating_numberofvotes_cleanedup.csv" , index=False)
    def data_actornames_cleanup() :
        dictionary_dict_actornames = []
        df1 = pd.read_csv ("data_actornames.tsv", chunksize=50000 ,sep= "\t")
        for chunks in df1 :
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
                            remade_dictionary = { "nconst" : f"{nconst}" , "primaryName" : f"{primaryName}" , "birthYear" : f"{birthYear}" , "deathYear" : f"{deathYear}" , "primaryProfession" : f"{primaryProfession}" , "knownForTitles" : f"{knownForTitles}"}
                            dictionary_dict_actornames.append(remade_dictionary)
        final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
        final_dataframe.to_csv("F:/taklif/cleaned_up/data_actornames_cleanedup.csv" , index=True)
    def data_character_jobs_cleanup() :
        df1 = pd.read_csv ("data_character_jobs.tsv", chunksize=500000 ,low_memory=False,sep= "\t")
        for chunk in df1 :            
            chunk.to_csv("F:/taklif/cleaned_up/data_character_jobs_cleanedup.csv" , index=True)

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
        from_tsv_to_sql.data_seasonnumber_episodenumber_cleanup()
        from_tsv_to_sql.data_title_and_labguages_cleanup()
        from_tsv_to_sql.data_isadult_runtime_cleanup()
        from_tsv_to_sql.data_directors_writers_cleanup()
        from_tsv_to_sql.data_averagerating_numberofvotes_cleanup()
        from_tsv_to_sql.data_actornames_cleanup()
        from_tsv_to_sql.data_character_jobs_cleanup()
    else :
        print ("option not available ")


