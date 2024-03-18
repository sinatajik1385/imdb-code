import psycopg2
import os
from file_normalizer_2nf import file_normalization , from_tsv_to_sql

directory = os.getcwd()
full_directory = f"{directory}/cleaned_up_nf2"
print (os.path.isdir(full_directory))
conn = psycopg2.connect(
        host = "localhost",
        dbname = "imdb",
        port = "5432",
        user = "postgres",
        password = "QaZxcv54321"
)

curr = conn.cursor()


"""
-- titleEpisode (tconst,parenttconst,seasonnumber,episodenumber)
-- titleAkas_region_and_languages (akas_region_and_languages_pk,titleId,title,region,language)
-- titleAkas_types (titleId,types_)
-- titleAkas_attributes (titleId,attributes)
-- titleAkas_isOriginalTitle(titleId,isOriginalTitle)
-- titleBasics_cleanup (tconst,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes)
-- titleBasics_genres (titleBasics_genres_pk,tconst,genres)
-- titleCrew_directors (titleCrew_directors_pk,tconst,directors)
-- titleCrew_writers (titleCrew_writers_pk,tconst,writers)
-- titleRatings (tconst,averageRating,numVotes)
-- nameBasics_general (nconst,primaryName,birthYear,deathYear)
-- nameBasics_primaryProfession (nameBasics_primaryProfession_pk,nconst,primaryProfession)
-- nameBasics_knownForTitles (nameBasics_knownForTitles_pk,nconst,knownForTitles)
-- titlePrincipals_general (titlePrincipals_general_pk,tconst,nconst,category)
-- titlePrincipals_job_cleanup (titlePrincipals_job_pk,tconst,nconst,job)
-- titlePrincipals_characters (titlePrincipals_characters_pk,tconst,nconst,characters_)
"""

#cleaning up

curr.execute("""

DROP TABLE IF EXISTS titleEpisode;
DROP TABLE IF EXISTS titleAkas_region_and_languages;
DROP TABLE IF EXISTS titleAkas_types;
DROP TABLE IF EXISTS titleAkas_attributes;
DROP TABLE IF EXISTS titleAkas_isOriginalTitle;
DROP TABLE IF EXISTS titleBasics_cleanup;
DROP TABLE IF EXISTS titleBasics_genres;
DROP TABLE IF EXISTS titleCrew_directors;
DROP TABLE IF EXISTS titleCrew_writers;
DROP TABLE IF EXISTS titleRatings;
DROP TABLE IF EXISTS nameBasics_general;
DROP TABLE IF EXISTS nameBasics_primaryProfession;
DROP TABLE IF EXISTS titlePrincipals_general;
DROP TABLE IF EXISTS titlePrincipals_job_cleanup;
DROP TABLE IF EXISTS titlePrincipals_job_cleanup;
            
""")

#making the data tables
if os.path.isfile(f"{full_directory}/titleEpisode.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleEpisode_cleanup()

        curr.execute("""
        CREATE TABLE titleEpisode ( 
            tconst VARCHAR NOT NULL PRIMARY KEY UNIQUE,  
            parentTconst VARCHAR NOT NULL ,  
            seasonNumber VARCHAR NOT NULL,  
            episodeNumber VARCHAR NOT NULL 
        ); 
        """)
else :
    curr.execute("""
    CREATE TABLE titleEpisode ( 
        tconst VARCHAR NOT NULL PRIMARY KEY UNIQUE,  
        parentTconst VARCHAR NOT NULL ,  
        seasonNumber VARCHAR NOT NULL,  
        episodeNumber VARCHAR NOT NULL 
    ); 
    """)

if os.path.isfile(f"{full_directory}/titleAkas_region_and_languages.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleAkas_region_and_languages_cleanup()

        curr.execute("""
        CREATE TABLE titleAkas_region_and_languages (
        akas_region_and_languages_pk VARCHAR NOT NULL PRIMARY KEY UNIQUE,
        titleId VARCHAR NOT NULL,
        title VARCHAR,
        region VARCHAR, 
        language VARCHAR
    );
        """)
else :
    curr.execute("""
    CREATE TABLE titleAkas_region_and_languages (
    akas_region_and_languages_pk VARCHAR NOT NULL PRIMARY KEY UNIQUE,
    titleId VARCHAR NOT NULL,
    title VARCHAR,
    region VARCHAR, 
    language VARCHAR
);
    """)

if os.path.isfile(f"{full_directory}/titleAkas_types.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleAkas_types_cleanup()

        curr.execute("""
        CREATE TABLE titleAkas_types (
            titleId VARCHAR NOT NULL PRIMARY KEY UNIQUE,
            types_ VARCHAR NOT NULL 
        );
        """)
else :
    curr.execute("""
    CREATE TABLE titleAkas_types (
    titleId VARCHAR NOT NULL PRIMARY KEY UNIQUE,
    types_ VARCHAR NOT NULL 
    );
    """)

if os.path.isfile(f"{full_directory}/titleAkas_isOriginalTitle.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleAkas_isOriginalTitle_cleanup()

        curr.execute("""
        CREATE TABLE titleAkas_isOriginalTitle(
        titleId VARCHAR NOT NULL PRIMARY KEY UNIQUE,
        isOriginalTitle VARCHAR NOT NULL
        );
        """)
else :
    curr.execute("""
    CREATE TABLE titleAkas_isOriginalTitle(
    titleId VARCHAR NOT NULL PRIMARY KEY UNIQUE,
    isOriginalTitle VARCHAR NOT NULL
    );
    """)

if os.path.isfile(f"{full_directory}/titleBasics.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleBasics_cleanup()

        curr.execute("""
        CREATE TABLE titleBasics(
        tconst varchar not null PRIMARY KEY, 
        titleType varchar not null, 
        primaryTitle  varchar , 
        originalTitle varchar , 
        isAdult varchar , 
        startYear varchar, 
        endYear varchar, 
        runtimeMinutes VARCHAR 
        );
        """)
else :
    curr.execute("""
    CREATE TABLE titleBasics(
    tconst varchar not null PRIMARY KEY, 
    titleType varchar not null, 
    primaryTitle  varchar , 
    originalTitle varchar , 
    isAdult varchar , 
    startYear varchar, 
        endYear varchar, 
        runtimeMinutes VARCHAR 
        ); 
        """)

if os.path.isfile(f"{full_directory}/titleBasics_genres.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleBasics_genres_cleanup()

        curr.execute("""
        CREATE TABLE titleBasics_genres(
        titleBasics_genres_pk INT NOT NULL PRIMARY KEY UNIQUE,
        tconst VARCHAR NOT NULL,
        genres VARCHAR NOT NULL
        ); 
        """)
else :
    curr.execute("""
    CREATE TABLE titleBasics_genres(
    titleBasics_genres_pk INT NOT NULL PRIMARY KEY UNIQUE,
    tconst VARCHAR NOT NULL,
    genres VARCHAR NOT NULL
    );
    """)

if os.path.isfile(f"{full_directory}/titleCrew_directors.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleCrew_directors_cleanup()

        curr.execute("""
        CREATE TABLE titleCrew_directors (
        titleCrew_directors_pk INT NOT NULL PRIMARY KEY UNIQUE,
        tconst VARCHAR NOT NULL,
        directors VARCHAR NOT NULL
        );
        """)
else :
    curr.execute("""
    CREATE TABLE titleCrew_directors (
    titleCrew_directors_pk INT NOT NULL PRIMARY KEY UNIQUE,
    tconst VARCHAR NOT NULL,
    directors VARCHAR NOT NULL
    );
    """)

if os.path.isfile(f"{full_directory}/titleCrew_writers.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleCrew_writers_cleanup()

        curr.execute("""
        CREATE TABLE titleCrew_writers(
        titleCrew_writers_pk INT NOT NULL PRIMARY KEY UNIQUE,
        tconst VARCHAR NOT NULL,
        writers VARCHAR NOT NULL
        );
    
        """)
else :
    curr.execute("""
    CREATE TABLE titleCrew_writers(
    titleCrew_writers_pk INT NOT NULL PRIMARY KEY UNIQUE,
    tconst VARCHAR NOT NULL,
    writers VARCHAR NOT NULL
    );

    """)
if os.path.isfile(f"{full_directory}/titleRatings.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titleRatings_cleanup()

        curr.execute("""
        Create table titleRatings( 
        tconst varchar PRIMARY KEY, 
        averageRating float NOT NULL,
        numVotes int NOT NULL
        );
        """)
else :
    curr.execute("""
    Create table titleRatings( 
    tconst varchar PRIMARY KEY, 
    averageRating float NOT NULL,
    numVotes int NOT NULL
    );
    """)

if os.path.isfile(f"{full_directory}/nameBasics_general.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.nameBasics_general_cleanup()

        curr.execute("""
        CREATE TABLE nameBasics_general(
        nconst VARCHAR NOT NULL PRIMARY KEY UNIQUE,
        primaryName VARCHAR NOT NULL,
        birthYear INT NOT NULL,
        deathYear VARCHAR
        );
        """)
else :
    curr.execute("""
    CREATE TABLE nameBasics_general(
    nconst VARCHAR NOT NULL PRIMARY KEY UNIQUE,
    primaryName VARCHAR NOT NULL,
    birthYear INT NOT NULL,
    deathYear VARCHAR
    ); 
    """)
if os.path.isfile(f"{full_directory}/nameBasics_primaryProfession.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.nameBasics_primaryProfession_cleanup()

        curr.execute("""
        CREATE TABLE nameBasics_primaryProfession(
        nameBasics_primaryProfession_pk INT NOT NULL PRIMARY KEY UNIQUE,
        nconst VARCHAR NOT NULL,
        primaryProfession VARCHAR
        );
        """)
else :
    curr.execute("""
    CREATE TABLE nameBasics_primaryProfession(
    nameBasics_primaryProfession_pk INT NOT NULL PRIMARY KEY UNIQUE,
    nconst VARCHAR NOT NULL,
    primaryProfession VARCHAR
    );
    """)
    
if os.path.isfile(f"{full_directory}/nameBasics_knownForTitles.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.nameBasics_knownForTitles_cleanup()

        curr.execute("""
        CREATE TABLE nameBasics_knownForTitles(
        nameBasics_knownForTitles_pk INT NOT NULL PRIMARY KEY UNIQUE,
        nconst VARCHAR NOT NULL,
        knownForTitles VARCHAR
        ); 
        """)
else :
    curr.execute("""
    CREATE TABLE nameBasics_knownForTitles(
    nameBasics_knownForTitles_pk INT NOT NULL PRIMARY KEY UNIQUE,
    nconst VARCHAR NOT NULL,
    knownForTitles VARCHAR
    );
    """)
    
if os.path.isfile(f"{full_directory}/titlePrincipals_general.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titlePrincipals_general_cleanup()

        curr.execute("""
        CREATE TABLE titlePrincipals_general(
        titlePrincipals_general_pk INT NOT NULL PRIMARY KEY UNIQUE,
        tconst VARCHAR NOT NULL,
        nconst VARCHAR NOT NULL,
        category VARCHAR
        );
        """)
else :
    curr.execute("""
    CREATE TABLE titlePrincipals_general(
    titlePrincipals_general_pk INT NOT NULL PRIMARY KEY UNIQUE,
    tconst VARCHAR NOT NULL,
    nconst VARCHAR NOT NULL,
    category VARCHAR
    );
    """)

if os.path.isfile(f"{full_directory}/titlePrincipals_job.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titlePrincipals_job_cleanup()

        curr.execute("""
        CREATE TABLE titlePrincipals_job(
        titlePrincipals_job_pk INT NOT NULL PRIMARY KEY UNIQUE,
        tconst VARCHAR NOT NULL,
        nconst VARCHAR
        );
        """)
else :
    curr.execute("""
    CREATE TABLE titlePrincipals_job(
    titlePrincipals_job_pk INT NOT NULL PRIMARY KEY UNIQUE,
    tconst VARCHAR NOT NULL,
    nconst VARCHAR
    );
    """)

if os.path.isfile(f"{full_directory}/titlePrincipals_characters.csv") == False :
    print ("database is missing")
    conformation = input("please press 1 to confirm : ")
    if conformation == "1" :
        from_tsv_to_sql.titlePrincipals_characters_cleanup()

        curr.execute("""
        CREATE TABLE titlePrincipals_characters(
        titlePrincipals_characters_pk INT NOT NULL PRIMARY KEY UNIQUE,
        tconst VARCHAR NOT NULL,
        nconst VARCHAR NOT NULL
        );
        """)
else :
    curr.execute("""
    CREATE TABLE titlePrincipals_characters(
    titlePrincipals_characters_pk INT NOT NULL PRIMARY KEY UNIQUE,
    tconst VARCHAR NOT NULL,
    nconst VARCHAR NOT NULL
    );
    """)

"""---------making the initial databses(2nf)---------"""

curr.execute (f"""
copy titleEpisode 
FROM '{full_directory}/titleEpisode.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleAkas_region_and_languages 
FROM '{full_directory}/titleAkas_region_and_languages.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleAkas_types 
FROM '{full_directory}/titleAkas_types.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleAkas_attributes 
FROM '{full_directory}/titleAkas_attributes.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleAkas_isOriginalTitle 
FROM '{full_directory}/titleAkas_isOriginalTitle.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleBasics_cleanup  
FROM '{full_directory}/titleBasics_cleanup .csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleBasics_genres
FROM '{full_directory}/titleBasics_genres.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleCrew_directors
FROM '{full_directory}/titleCrew_directors.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleCrew_writers 
FROM '{full_directory}/titleCrew_writers.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titleRatings
FROM '{full_directory}/titleRatings.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy nameBasics_general 
FROM '{full_directory}/nameBasics_general.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy nameBasics_primaryProfession 
FROM '{full_directory}/nameBasics_primaryProfession.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy nameBasics_knownForTitles 
FROM '{full_directory}/nameBasics_knownForTitles.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titlePrincipals_general 
FROM '{full_directory}/titlePrincipals_general.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titlePrincipals_job_cleanup 
FROM '{full_directory}/titlePrincipals_job_cleanup.csv' 
DELIMITER E',' 
CSV HEADER;

""")

curr.execute (f"""
copy titlePrincipals_characters 
FROM '{full_directory}/titlePrincipals_characters.csv' 
DELIMITER E',' 
CSV HEADER;

""")


conn.commit()

curr.close()
conn.close()
