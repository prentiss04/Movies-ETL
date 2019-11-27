#!/usr/bin/env python
# coding: utf-8

#############################################################################
# Import dependencies
#############################################################################

import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
from config import db_password
import time

#############################################################################
# Define Variables/Actions
#############################################################################

file_dir = 'C:/Users/prent/DSBC/Movies-ETL'

###################################DELETE THIS######################################


print ("Process Starting")


#############################################################################
# Function
#############################################################################

def movie_etl(wiki_migration, kaggle_migration, movielens_migration):
    # clean-up wiki data
        # open the json file
        with open(f'{file_dir}/{wiki_migration}', mode='r') as file:
            wiki_movies_raw = json.load(file)
#################################################################################################
        wiki_movies_df = pd.DataFrame(wiki_movies_raw)
        wiki_movies = [movie for movie in wiki_movies_raw
                    if ('Director' in movie or 'Directed by' in movie)
                    and 'imdb_link' in movie
                    and 'No. of episodes' not in movie]

        wiki_movies_df = pd.DataFrame(wiki_movies)

        def clean_movie(movie):
            movie = dict(movie) #create a non-destructive copy
            alt_titles = {}

            try:

                for key in ['Also known as', 'Arabic', 'Cantonese', 'Chinese', 'French',
                        'Hangul', 'Hebrew', 'Hepburn', 'Japanese', 'Literally',
                        'Mandarin', 'McCune–Reischauer', 'Original title', 'Polish',
                        'Revised Romanization', 'Romanized', 'Russian', 'Simplified',
                        'Traditional', 'Yiddish']:
                    if key in movie:
                        alt_titles[key] = movie[key]
                        movie.pop(key)
                if len(alt_titles) > 0:
                    movie['alt_titles'] = alt_titles

                # merge column names
                def change_column_name(old_name, new_name):
                    if old_name in movie:
                        movie[new_name] = movie.pop(old_name)
                change_column_name('Adaptation by', 'Writer(s)')
                change_column_name('Country of Origin', 'Country')
                change_column_name('Directed by', 'Director')
                change_column_name('Distributed by', 'Distributor')
                change_column_name('Edited by', 'Editor(s)')
                change_column_name('Length', 'Running Time')
                change_column_name('Original release', 'Release date')
                change_column_name('Music by', 'Composer(s)')
                change_column_name('Produced by', 'Producer(s)')
                change_column_name('Producer', 'Producer(s)')
                change_column_name('Productioncompanies ', 'Production company(s)')
                change_column_name('Productioncompany ', 'Production company(s)')
                change_column_name('Prodcutioncompanies', 'Production company(s)')
                change_column_name('Productioncompany', 'Production company(s)')
                change_column_name('Released', 'Release Date')
                change_column_name('Release Date', 'Release date')
                change_column_name('Screen story by', 'Release date')
                change_column_name('Screenplay by', 'Writer(s)')
                change_column_name('Story by', 'Writer(s)')
                change_column_name('Theme music composer', 'Composer(s)')
                change_column_name('Written by', 'Writer(s)')

                return movie

            except:
                print("Column error")

        clean_movies = [clean_movie(movie) for movie in wiki_movies]

        wiki_movies_df = pd.DataFrame(clean_movies)
        sorted(wiki_movies_df.columns.tolist())

        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        wiki_movies_df = pd.DataFrame(clean_movies)

        # dropping duplicate rows
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

        # # Box office clean-up

        box_office = wiki_movies_df['Box office'].dropna().copy()
        #box_office

        # return row output that is not a string
        def is_not_a_string(x):
            return type(x) != str

        #box_office[box_office.map(is_not_a_string)]
        #box_office[box_office.map(lambda x: type(x) != str)]

        box_office = box_office.apply(lambda x: " ".join(x) if type(x) == list else x)

        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'

        box_office.str.contains(form_one, flags=re.IGNORECASE).sum()

        form_two = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)"

        box_office.str.contains(form_two, flags=re.IGNORECASE).sum()

        # Had to copy "–" from the output as the regular "-" is a different character and didn't affect results.
        box_office = box_office.str.replace(r'\$.*[–––](?![a-z])', '$', regex=True)

        def parse_dollars(s):
            # if s is not a string, return NaN
            if type(s) != str:
                return np.nan

            # if input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):


                # remove dollar sign and " million"
                s = re.sub('\$|\s||[a-zA-Z]', '', s)

                # convert to float and multiply by a million
                value = float(s) * 10**6

                # return value
                return value

            # if input is of the form $###.# billion
            if re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

                # remove dollar sign and " billion"
                s = re.sub('\$|\s|[a-zA-Z]', '', s)

                # convert to float and multiply by a billion
                value = float(s) * 10**9

                # return value
                return value

            # if input is of the form $###,###,###
            elif re.match(r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)" , s, flags=re.IGNORECASE):

                # remove dollar sign and commas
                s = re.sub('\$|,','', s)

                # convert to float
                value = float(s)

                # return value
                return value

            # otherwise, return NaN
            else:
                return np.nan

        # Extract box_office values
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        wiki_movies_df.drop('Box office', axis=1, inplace=True)

        # # Budget clean-up

        budget = wiki_movies_df['Budget'].dropna()

        budget = budget.map(lambda x: " ".join(x) if type(x) == list else x)

        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        # remove citations
        budget = budget.str.replace(r'\[\d+\]\s*','')

        wiki_movies_df['budget'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        # Drop "Budget" column
        wiki_movies_df.drop('Budget', axis=1, inplace=True)

        # # Release date clean-up

        release_date_wiki = wiki_movies_df['Release date'].dropna().apply(lambda x: " ".join(x) if type(x) == list else x)

        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = f'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'

        #release_date.str.extract(f'({date_form_one})', flags=re.IGNORECASE)
        #release_date_wiki.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)

        # Assign parsed release dates to datetime format
        wiki_movies_df['release_date_wiki'] = pd.to_datetime(release_date_wiki.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

        # # Running time clean-up

        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: " ".join(x) if type(x) == list else x)

        # running_time[running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE) != True]

        running_time.str.contains(r'\d*\s*m', flags=re.IGNORECASE).sum()

        running_time[running_time.str.contains(r'\d*\s*m', flags=re.IGNORECASE) != True]

        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

        running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

        wiki_movies_df.drop('Running time', axis=1, inplace=True)

        wiki_movies_df['running_time'] = pd.to_numeric(wiki_movies_df['running_time'])

#################################################################################################################

    # clean-up kaggle data
        # call a bunch of kaggle actions
        # read the kaggle csv
        kaggle_metadata = pd.read_csv(f'{file_dir}/{kaggle_migration}', low_memory=False)

        # clean up kaggle DF

        try:
            kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]

            kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult', axis='columns')

            kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'

            kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
            kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
            kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
            kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

        except:
                print("kaggle metadata error")


        # rename the columns

    # clean-up movielens data
        # read the ratings csv
        ratings = pd.read_csv(f'{file_dir}/{movielens_migration}')


        # # Ratings clean-up

        try:
            pd.to_datetime(ratings['timestamp'], unit='s')

            ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
        except:
            print("Movielens data error")


        # group ratings
        # pivot ratings
        # merge ratings into DF
        # fill in blanks with zero

    # # Merging datasets

        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki', '_kaggle'])

        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date'] < '1965-01-01')].index)

        movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)

        movies_df['original_language'].value_counts(dropna=False)

        movies_df.drop(columns=['title_wiki', 'Release date', 'Language', 'Production company(s)'], inplace=True)

        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
                , axis=1)
            df.drop(columns=wiki_column, inplace=True)

        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')

        for col in movies_df.columns:
            lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
            value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
            num_values = len(value_counts)
            if num_values ==1:
                print(col)

        movies_df['video'].value_counts(dropna=False)

        movies_df = movies_df[['imdb_id', 'id', 'title_kaggle', 'original_title', 'tagline', 'belongs_to_collection', 'url', 
                            'imdb_link', 'runtime','budget_kaggle','revenue','release_date','popularity','vote_average','vote_count',
                            'genres','original_language','overview','spoken_languages','Country',
                            'production_companies','production_countries','Distributor',
                            'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                            ]]

        movies_df.rename({'id':'kaggle_id',
                        'title_kaggle':'title',
                        'url':'wikipedia_url',
                        'budget_kaggle':'budget',
                        'release_date_kaggle':'release_date',
                        'Country':'country',
                        'Distributor':'distributor',
                        'Producer(s)':'producers',
                        'Director':'director',
                        'Starring':'starring',
                        'Cinematography':'cinematography',
                        'Editor(s)':'editors',
                        'Writer(s)':'writers',
                        'Composer(s)':'composers',
                        'Based on':'based_on'
                        }, axis='columns', inplace=True)


        # # Ratings optimizing

        rating_counts = ratings.groupby(['movieId', 'rating'], as_index=False).count().rename({'userId':'count'}, axis=1).pivot(index='movieId', columns='rating', values='count')

        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

        print ("Steps complete")

        # # Importing modules to pgadmin

        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

        engine = create_engine(db_string)

        # movie_data already exists so this code won't work.
        movies_df.to_sql(name='movie_data', con=engine)

        ### remember to uncomment this line of code
        ########################################################################################

        # create a variable for the number of rows imported
        rows_imported = 0

        # get the start_time from time.time()
        start_time = time.time()

        for data in pd.read_csv(f'{file_dir}/{movielens_migration}', chunksize=500000):

            # print out the range of rows that are being imported
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')

            data.to_sql(name='ratings', con=engine, if_exists='append')

            # increment the number of rows imported by the size of 'data'
            rows_imported += len(data)

            # print that the rows have finished importing

            print(f'Done. {time.time() - start_time} total seconds elapsed')

# call movie_etl fxn

movie_etl('wikipedia-movies.json', 'movies_metadata.csv', 'ratings.csv')


