import pandas as pd
import numpy as np
import os
class from_tsv_to_sql ():
# this already is 2nf and the primary key is the tconst
    def titleEpisode_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titleEpisode.csv") == True :
            print ("file already exists")
        else:
            header = 1
            df = pd.read_csv ("titleEpisode.tsv",chunksize=50000, sep= "\t")
            for chunks in df :
                dictionary_seasonnumber_episodenumber = []
                chunks.drop_duplicates()
                data_dict_seasonnumber_episodenumber = chunks.to_dict(orient = "records")
                for i in data_dict_seasonnumber_episodenumber :  
                    dictionary_seasonnumber_episodenumber.append(i)
                if header == 1 :
                    header += 1 
                    final_dataframe = pd.DataFrame.from_dict(dictionary_seasonnumber_episodenumber)
                    final_dataframe.to_csv(f"{directory}/titleEpisode.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_seasonnumber_episodenumber)
                    final_dataframe.to_csv(f"{directory}/titleEpisode.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  ttile akas will be turnd into akas_region_and_languages , akas_types , akas_attributes , akas_isOriginalTitle
#  this part normalizes akas_types which includes the title id, ordering ,  title , region , language 
    def titleAkas_region_and_languages_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/Akas_region_and_languages.csv") == True :
            print ("file already exists")
        else :
            header = 1
            akas_region_and_languages_pk = 1
            df = pd.read_csv ("titleAkas.tsv", chunksize=50000 ,sep= "\t")
            for chunks in df :
                dictionary_dict_title_and_labguages = []
                chunks.drop_duplicates()
                data_dict_title_and_labguages = chunks.to_dict(orient= "records")
                for i in data_dict_title_and_labguages :
                    titleId =  i["titleId"]
                    title = i["title"]
                    region = i["region"]
                    language = i["language"]
                    remade_dictionary = { "akas_region_and_languages_pk" : f"{akas_region_and_languages_pk}" ,"titleId" : f"{titleId}" , "title" : f"{title}" , "region" : f"{region}" , "language" : F"{language}" }
                    dictionary_dict_title_and_labguages.append(remade_dictionary)
                    akas_region_and_languages_pk += 1 
                if header == 1 :
                    header +=1 
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/Akas_region_and_languages.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/Akas_region_and_languages.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  this part normalizes akas_types which includes the title id, types 
    def titleAkas_types_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/akas_types_.csv") == True :
            print ("file already exists")
        else :
            header = 1
            df = pd.read_csv ("titleAkas.tsv", chunksize=50000 ,sep= "\t")
            for chunks in df :
                dictionary_dict_title_and_labguages = []
                chunks.drop_duplicates()
                data_dict_title_and_labguages = chunks.to_dict(orient= "records")
                for i in data_dict_title_and_labguages :
                    if i["types"] == "\\N" :
                            pass
                    else :
                        titleId =  i["titleId"]
                        types_ = i["types"]
                        remade_dictionary = {"titleId" : f"{titleId}" , "types_" : f"{types_}"}
                        dictionary_dict_title_and_labguages.append(remade_dictionary)
                if header == 1 :
                    header +=1 
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/akas_types_.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/akas_types_.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  this part normalizes akas_attributes which includes the title id, attributes 
    def titleAkas_attributes_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/akas_attributes.csv") == True :
            print ("file already exists")
        else :
            header = 1
            df = pd.read_csv ("titleAkas.tsv", chunksize=50000 ,sep= "\t")
            for chunks in df :
                dictionary_dict_title_and_labguages = []
                chunks.drop_duplicates()
                data_dict_title_and_labguages = chunks.to_dict(orient= "records")
                for i in data_dict_title_and_labguages :
                    if i["attributes"] == "\\N" :
                            pass
                    else :
                        titleId =  i["titleId"]
                        attributes = i["attributes"]
                        remade_dictionary = {"titleId" : f"{titleId}" , "attributes" : f"{attributes}"}
                        dictionary_dict_title_and_labguages.append(remade_dictionary)
                if header == 1 :
                    header +=1 
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/akas_attributes.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/akas_attributes.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  this part normalizes akas_isOriginalTitle which includes the title id, isOriginalTitle
    def titleAkas_isOriginalTitle_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/akas_isOriginalTitle.csv") == True :
            print ("file already exists")
        else :
            header = 1
            df = pd.read_csv ("titleAkas.tsv", chunksize=50000 ,sep= "\t")
            for chunks in df :
                dictionary_dict_title_and_labguages = []
                chunks.drop_duplicates()
                data_dict_title_and_labguages = chunks.to_dict(orient= "records")
                for i in data_dict_title_and_labguages :
                    if i["isOriginalTitle"] == "0" :
                        titleId =  i["titleId"]
                        isOriginalTitle = "it isnt original"
                        remade_dictionary = {"titleId" : f"{titleId}" , "isOriginalTitle" : f"{isOriginalTitle}"}
                        dictionary_dict_title_and_labguages.append(remade_dictionary)
                    else :
                        titleId =  i["titleId"]
                        isOriginalTitle = "it is original"
                        remade_dictionary = {"titleId" : f"{titleId}" , "isOriginalTitle" : f"{isOriginalTitle}"}
                        dictionary_dict_title_and_labguages.append(remade_dictionary)
                if header == 1 :
                    header +=1 
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/akas_isOriginalTitle.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_title_and_labguages)
                    final_dataframe.to_csv(f"{directory}/akas_isOriginalTitle.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  ttile akas will be turnd into titleBasics , titleBasics_genres
#  this part normalizes titleBasics which includes the tconst, titleType , primaryTitle, originalTitle , isAdult , startYear , endYear , runtimeMinutes 
    def titleBasics_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titleBasics.csv") == True :
            print ("file already exists")
        else :     
            df = pd.read_csv ("titleBasics.tsv", chunksize=50000 ,sep= "\t")
            header = 1
            for chunks in df :
                dictionary_dict_isadult_runtime = []
                chunks.drop_duplicates()
                data_dict_isadult_runtime = chunks.to_dict(orient= "records")
                for i in data_dict_isadult_runtime :
                    tconst = i["tconst"]
                    titleType = i["titleType"]
                    primaryTitle = i["primaryTitle"]
                    originalTitle = i["originalTitle"]
                    if i["isAdult"] == 0 :
                        isAdult = "it isnt adult"
                    else :
                            isAdult = "it is adult"
                    startYear = i["startYear"]
                    endYear = i["endYear"]
                    runtimeMinutes = i["runtimeMinutes"]
                    remade_dictionary = {"tconst" : f"{tconst}" , "titleType" : f"{titleType}" , "primaryTitle" : f"{primaryTitle}" , "originalTitle" : f"{originalTitle}" , "isAdult" : F"{isAdult}" , "startYear" : f"{startYear}" , "endYear" : f"{endYear}" , "runtimeMinutes" : f"{runtimeMinutes}"}
                    dictionary_dict_isadult_runtime.append(remade_dictionary)
                if header == 1 :
                    header += 1                     
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_isadult_runtime)
                    final_dataframe.to_csv(f"{directory}/titleBasics.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_isadult_runtime)
                    final_dataframe.to_csv(f"{directory}/titleBasics.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  this part normalizes titleBasics_genres which includes the titleBasics_genres_pk , tconst , genres
    def titleBasics_genres_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titleBasics_genres.csv") == True :
            print ("file already exists")
        else :     
            df = pd.read_csv ("titleBasics.tsv", chunksize=50000 ,sep= "\t")
            titleBasics_genres_genres_pk = 1
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
                            genres = data_number_of_genres
                            remade_dictionary = { "titleBasics_genres_genres_pk" : f"{titleBasics_genres_genres_pk}" ,"tconst" : f"{tconst}" , "genres" : f"{genres}"}
                            dictionary_dict_isadult_runtime.append(remade_dictionary)
                            titleBasics_genres_genres_pk += 1
                if header == 1 :
                    header += 1                     
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_isadult_runtime)
                    final_dataframe.to_csv(f"{directory}/titleBasics_genres.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_isadult_runtime)
                    final_dataframe.to_csv(f"{directory}/titleBasics_genres.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  ttile crew will be turnd into titleCrew_directors , titleCrew_writers
#  this part normalizes titleCrew_directors which includes titleCrew_directors_pk , tconst , directors 
    def titleCrew_directors_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titleCrew_directors.csv") == True :
            print ("file already exists")
        else :
            titleCrew_directors_pk = 1
            header = 1
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
                        for data_number_of_directors in number_of_directors   :
                            tconst = i["tconst"]
                            directors = data_number_of_directors
                            remade_dictionary = { "titleCrew_directors_pk" : f"{titleCrew_directors_pk}" ,"tconst" : f"{tconst}" , "directors" : f"{directors}"}
                            dictionary_dict_directors_writers.append(remade_dictionary)
                            titleCrew_directors_pk += 1 
                if header == 1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_directors_writers)
                    final_dataframe.to_csv(f"{directory}/titleCrew_directors.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_directors_writers)
                    final_dataframe.to_csv(f"{directory}/titleCrew_directors.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  this part normalizes titleCrew_writers which includes the titleCrew_writers_pk , tconst , writers
    def titleCrew_writers_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titleCrew_writers.csv") == True :
            print ("file already exists")
        else :
            titleCrew_writers_pk = 1
            header = 1
            df = pd.read_csv ("titleCrew.tsv", chunksize=50000 ,sep= "\t")
            for chunks in df :
                dictionary_dict_directors_writers = []
                chunks.drop_duplicates()
                data_dict_directors_writers = chunks.to_dict(orient= "records")
                for i in data_dict_directors_writers :
                    if i["writers"] == "\\N" :
                            pass
                    else :
                        number_of_writers =  i["writers"].split(",")
                        for data_number_of_writers in number_of_writers   :
                            tconst = i["tconst"]
                            writers = data_number_of_writers
                            remade_dictionary = { "titleCrew_writers_pk" : f"{titleCrew_writers_pk}" ,"tconst" : f"{tconst}" , "writers" : f"{writers}"}
                            dictionary_dict_directors_writers.append(remade_dictionary)
                            titleCrew_writers_pk += 1 
                if header == 1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_directors_writers)
                    final_dataframe.to_csv(f"{directory}/titleCrew_writers.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_directors_writers)
                    final_dataframe.to_csv(f"{directory}/titleCrew_writers.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
# this database does not need furtehr normalization
    def titleRatings_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titleRatings.csv") == True :
            print ("file already exists")
        else :
            header = 1 
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
                if header ==1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_averagerating_numberofvotes)
                    final_dataframe.to_csv(f"{directory}/titleRatings.csv",mode="a",header=True,encoding='utf-8-sig' , index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_averagerating_numberofvotes)
                    final_dataframe.to_csv(f"{directory}/titleRatings.csv",mode="a",header=False,encoding='utf-8-sig' , index=False)
#  name basics will be turnd into nameBasics_general , nameBasics_primaryProfession , nameBasics_knownForTitles
#  this part normalizes nameBasics_general which includes the nconst , primaryName , birthYear , deathYear , 
    def nameBasics_general_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/nameBasics_general.csv") == True :
            print ("file already exists")
        else :
            df = pd.read_csv ("nameBasics.tsv", chunksize=50000 ,sep= "\t")
            header = 1
            for chunks in df :
                dictionary_dict_actornames = []
                chunks.drop_duplicates()
                data_dict_actornames = chunks.to_dict(orient= "records")
                for i in data_dict_actornames :
                    if i["birthYear"] == "\\N" :
                        pass
                    elif i["deathYear"] == "\\N" :
                        i["deathYear"] = "hasnt died yet"
                    else :
                        remade_dictionary = {}                        
                        nconst = i["nconst"]
                        primaryName = i["primaryName"] 
                        birthYear = i["birthYear"]
                        deathYear = i["deathYear"]
                        remade_dictionary = {"nconst" : f"{nconst}" , "primaryName" : f"{primaryName}" , "birthYear" : f"{birthYear}" , "deathYear" : f"{deathYear}"}
                        dictionary_dict_actornames.append(remade_dictionary)
                if header == 1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
                    final_dataframe.to_csv(f"{directory}/nameBasics_general.csv",mode="a",header=True,encoding='utf-8-sig',index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
                    final_dataframe.to_csv(f"{directory}/nameBasics_general.csv",mode="a",header=False,encoding='utf-8-sig',index=False)
#  this part normalizes nameBasics_primaryProfession which includes the nameBasics_primaryProfession_pk , nconst , primaryProfession
    def nameBasics_primaryProfession_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/nameBasics_primaryProfession.csv") == True :
            print ("file already exists")
        else :
            df = pd.read_csv ("nameBasics.tsv", chunksize=50000 ,sep= "\t")
            header = 1
            nameBasics_primaryProfession_pk = 1
            for chunks in df :
                dictionary_dict_actornames = []
                chunks.drop_duplicates()
                data_dict_actornames = chunks.to_dict(orient= "records")
                for i in data_dict_actornames :
                    if i["primaryProfession"] == "\\N" :
                        pass
                    else :
                        number_of_proffesions = str(i["primaryProfession"]).split(",")
                        for data_number_of_proffesions in number_of_proffesions:
                            remade_dictionary = {}                        
                            nconst = i["nconst"]
                            primaryProfession = data_number_of_proffesions
                            remade_dictionary = { "nameBasics_primaryProfession_pk" : f"{nameBasics_primaryProfession_pk}" ,"nconst" : f"{nconst}" , "primaryProfession" : f"{primaryProfession}" }
                            dictionary_dict_actornames.append(remade_dictionary)
                            nameBasics_primaryProfession_pk += 1
                if header == 1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
                    final_dataframe.to_csv(f"{directory}/nameBasics_primaryProfession.csv",mode="a",header=True,encoding='utf-8-sig',index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
                    final_dataframe.to_csv(f"{directory}/nameBasics_primaryProfession.csv",mode="a",header=False,encoding='utf-8-sig',index=False)  
#  this part normalizes nameBasics_knownForTitles which includes the nameBasics_knownForTitles_pk , nconst , knownForTitles
    def nameBasics_knownForTitles_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/nameBasics_knownForTitles.csv") == True :
            print ("file already exists")
        else :
            df = pd.read_csv ("nameBasics.tsv", chunksize=50000 ,sep= "\t")
            header = 1
            nameBasics_knownForTitles_pk = 1
            for chunks in df :
                dictionary_dict_actornames = []
                chunks.drop_duplicates()
                data_dict_actornames = chunks.to_dict(orient= "records")
                for i in data_dict_actornames :
                    if i["knownForTitles"] == "\\N" :
                        pass
                    else :
                        number_of_titles = str(i["knownForTitles"]).split(",")
                        for data_number_of_titles in number_of_titles:
                            remade_dictionary = {}                        
                            nconst = i["nconst"]
                            knownForTitles = data_number_of_titles
                            remade_dictionary = { "nameBasics_knownForTitles_pk" : f"{nameBasics_knownForTitles_pk}" ,"nconst" : f"{nconst}" , "knownForTitles" : f"{knownForTitles}" }
                            dictionary_dict_actornames.append(remade_dictionary)
                            nameBasics_knownForTitles_pk += 1
                if header == 1 :
                    header += 1
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
                    final_dataframe.to_csv(f"{directory}/nameBasics_knownForTitles.csv",mode="a",header=True,encoding='utf-8-sig',index=False)
                else :
                    final_dataframe = pd.DataFrame.from_dict(dictionary_dict_actornames)
                    final_dataframe.to_csv(f"{directory}/nameBasics_knownForTitles.csv",mode="a",header=False,encoding='utf-8-sig',index=False)  
#  titlePrincipals will be turnd into titlePrincipals_general , titlePrincipals_job , titlePrincipals_characters
#  this part normalizes titlePrincipals_general which includes the titlePrincipals_general_pk , tconst , nconst , category , job ,characters
    def titlePrincipals_general_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titlePrincipals_general.csv") == True :
            print ("file already exists")
        else :
            header = 1
            titlePrincipals_general_pk = 1 
            df = pd.read_csv ("titlePrincipals.tsv", chunksize=500000 ,low_memory=False,sep= "\t")
            for chunks in df : 
                dictionary_dict_titlePrincipals_general = []
                chunks.drop_duplicates()
                data_dict_titlePrincipals_general = chunks.to_dict(orient= "records")
                for i in data_dict_titlePrincipals_general :
                    remade_dictionary = {}                        
                    tconst = i["tconst"]
                    nconst = i["nconst"]
                    category = i["category"]
                    remade_dictionary = { "titlePrincipals_general_pk" : f"{titlePrincipals_general_pk}" ,"tconst" : f"{tconst}","nconst" : f"{nconst}","category" : f"{category}" }
                    dictionary_dict_titlePrincipals_general.append(remade_dictionary)
                    titlePrincipals_general_pk += 1
                if header == 1 :
                    header += 1      
                    chunks.to_csv(f"{directory}/titlePrincipals_general.csv",mode ="a",header=True,encoding='utf-8-sig' , index=True)
                else :
                    chunks.to_csv(f"{directory}/titlePrincipals_general.csv",mode ="a",header=False,encoding='utf-8-sig' , index=True)
#  this part normalizes titlePrincipals_job which includes the titlePrincipals_job_pk , tconst , nconst ,  job 
    def titlePrincipals_job_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titlePrincipals_job.csv") == True :
            print ("file already exists")
        else :
            header = 1
            titlePrincipals_job_pk = 1 
            df = pd.read_csv ("titlePrincipals.tsv", chunksize=500000 ,low_memory=False,sep= "\t")
            for chunks in df : 
                dictionary_dict_titlePrincipals_job = []
                chunks.drop_duplicates()
                data_dict_titlePrincipals_job = chunks.to_dict(orient= "records")
                for i in data_dict_titlePrincipals_job :
                    if i["job"] == "\\N" :
                        pass
                    else :
                        remade_dictionary = {}                        
                        tconst = i["tconst"]
                        nconst = i["nconst"]
                        job = i["job"]
                        remade_dictionary = { "titlePrincipals_job_pk" : f"{titlePrincipals_job_pk}" ,"tconst" : f"{tconst}","nconst" : f"{nconst}","job" : f"{job}" }
                        dictionary_dict_titlePrincipals_job.append(remade_dictionary)
                        titlePrincipals_job_pk += 1
                if header == 1 :
                    header += 1      
                    chunks.to_csv(f"{directory}/titlePrincipals_job.csv",mode ="a",header=True,encoding='utf-8-sig' , index=True)
                else :
                    chunks.to_csv(f"{directory}/titlePrincipals_job.csv",mode ="a",header=False,encoding='utf-8-sig' , index=True)
#  this part normalizes titlePrincipals_characters which includes the titlePrincipals_characters_pk , tconst , nconst ,  characters_ 
    def titlePrincipals_characters_cleanup() :
        directory = f"{directo}/cleaned_up_nf2"
        if os.path.isfile(f"{directory}/titlePrincipals_characters.csv") == True :
            print ("file already exists")
        else :
            header = 1
            titlePrincipals_characters_pk = 1 
            df = pd.read_csv ("titlePrincipals.tsv", chunksize=500000 ,low_memory=False,sep= "\t")
            for chunks in df : 
                dictionary_dict_titlePrincipals_characters = []
                chunks.drop_duplicates()
                data_dict_titlePrincipals_characters = chunks.to_dict(orient= "records")
                for i in data_dict_titlePrincipals_characters :
                    if i["characters"] == "\\N" :
                        pass
                    else :
                        remade_dictionary = {}                        
                        tconst = i["tconst"]
                        nconst = i["nconst"]
                        characters = i["characters"]
                        remade_dictionary = { "titlePrincipals_characters_pk" : f"{titlePrincipals_characters_pk}" ,"tconst" : f"{tconst}","nconst" : f"{nconst}","characters_" : f"{characters}" }
                        dictionary_dict_titlePrincipals_characters.append(remade_dictionary)
                        titlePrincipals_characters_pk += 1
                if header == 1 :
                    header += 1      
                    chunks.to_csv(f"{directory}/titlePrincipals_characters.csv",mode ="a",header=True,encoding='utf-8-sig' , index=True)
                else :
                    chunks.to_csv(f"{directory}/titlePrincipals_characters.csv",mode ="a",header=False,encoding='utf-8-sig' , index=True)
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
class file_normalization () :
    try :
        os.mkdir(f"{directo}/cleaned_up_nf2")
    except :
        pass 
    finally :
        while True : 
            menu = input(f"----------\n1.cleanup the data\n2.exit\n-----------\nplease enter : ")
            if menu == "1" :
                from_tsv_to_sql.titleEpisode_cleanup()
                print (f"titleEpisode is done \n ----------------------------- ")

                print (f"1st database is done \n ----------------------------- ")

                from_tsv_to_sql.titleAkas_types_cleanup()
                print (f"titleAkas_types is done \n ----------------------------- ")
                from_tsv_to_sql.titleAkas_region_and_languages_cleanup()
                print (f"titleAkas_region_and_languages is done \n ----------------------------- ")
                from_tsv_to_sql.titleAkas_attributes_cleanup()
                print (f"titleAkas_attributes is done \n ----------------------------- ")
                from_tsv_to_sql.titleAkas_isOriginalTitle_cleanup()
                print (f"titleAkas_isOriginalTitle is done \n ----------------------------- ")

                print (f"2nd database is done \n ----------------------------- ")

                from_tsv_to_sql.titleBasics_cleanup()
                print (f"titleBasics is done \n ----------------------------- ")
                from_tsv_to_sql.titleBasics_genres_cleanup()
                print (f"titleBasics_genres is done \n ----------------------------- ")

                print (f"3rd database is done \n ----------------------------- ")

                from_tsv_to_sql.titleCrew_directors_cleanup()
                print (f"titleCrew_directors is done \n ----------------------------- ")
                from_tsv_to_sql.titleCrew_writers_cleanup()
                print (f"titleCrew_writers is done \n ----------------------------- ")

                print (f"4th database is done \n ----------------------------- ")

                from_tsv_to_sql.titleRatings_cleanup()
                print (f"titleRatings is done \n ----------------------------- ")

                print (f"5th database is done \n ----------------------------- ")

                from_tsv_to_sql.nameBasics_general_cleanup()
                print (f"nameBasics_general is done \n ----------------------------- ")
                from_tsv_to_sql.nameBasics_knownForTitles_cleanup()
                print (f"nameBasics_knownForTitles is done \n ----------------------------- ")
                from_tsv_to_sql.nameBasics_primaryProfession_cleanup()
                print (f"nameBasics_primaryProfession is done \n ----------------------------- ")

                print (f"6th database is done \n ----------------------------- ")
                
                from_tsv_to_sql.titlePrincipals_general_cleanup()
                print (f"titlePrincipals_general is done \n ----------------------------- ")
                from_tsv_to_sql.titlePrincipals_general_cleanup()
                print (f"titlePrincipals_general is done \n ----------------------------- ")
                from_tsv_to_sql.titlePrincipals_job_cleanup()
                print (f"titlePrincipals_job is done \n ----------------------------- ")

                print (f"7th database is done \n ----------------------------- ")
            if menu == "2" :
                pass
            else :
                print ("option not available ")
