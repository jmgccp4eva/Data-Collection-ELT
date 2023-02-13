import time
import json
import urllib
from urllib import request
from urllib import parse
import sqlite3
import requests

import config
import data_collection
import sqliteDBProcessing
import concurrent.futures as futures

def create_data_db(db,new_db):
    # create_series_in_data_db(db,new_db)
    # create_episodes_in_data_db(db,new_db)
    # create_g_n_pc_and_c_in_data_db(db,new_db)
    # create_movies_in_data_db(db, new_db)
    # create_movie_collections_in_data_db(db,new_db)
    # create_movie_genres_in_data_db(db, new_db)
    # create_movie_pc_in_data_db(db, new_db)
    # create_series_g_n_pc_in_data_db(db, new_db)
    # create_episode_casts_in_data_db(db, new_db)
    # create_movie_casts(db,new_db)
    # get_overview_series()
    # get_overview_episodes()
    # get_overview_movies()
    # insert_overview(new_db,'seriesOverviews',['id','seriesID','overview'],['INTEGER PRIMARY KEY','INT','TEXT'],
    #                 ['seriesID'],['series_overview_series_id_index'],'series-overviews.tsv')
    # insert_overview(new_db,'moviesOverviews',['id','movieID','overview'],['INTEGER PRIMARY KEY','INT','TEXT'],
    #                 ['movieID'],['movies_overview_movies_id_index'],'movie_overviews.tsv')
    # insert_overview(new_db,'episodesOverviews',['id','episodeID','overview'],['INTEGER PRIMARY KEY','INT','TEXT'],
    #                 ['episodeID'],['episodes_overview_episodes_id_index'],'episodes_overview.tsv')
    # get_all_release_dates_for_movies()
    # insert_release_dates_for_movies(new_db,'movieReleaseDates',['id','movieID','rating','release_date','type'],
    #                                 ['INTEGER PRIMARY KEY','INT','TEXT','TEXT','INT'])
    # find_duplicate('duplicate-movies.tsv','movies')
    # find_duplicate('duplicate-series.tsv','series')
    # find_duplicate('duplicate-episodes.tsv','episodes')
    # delete_duplicate_series_episodes_and_casts(new_db)
    # delete_duplicate_movies(new_db)
    # get_imdb_ids_for_series(new_db)
    # get_imdb_ids_for_episodes(new_db)
    # get_streaming_services(new_db)
    # get_series_streamers(new_db,'where_to_watch')
    # get_movie_streamers(new_db,'where_to_watch')
    # download_all_movies(new_db)
    convert_series_movies_and_episodes_to_one_table_to_be_exported(new_db)







    # qry = "SELECT series.name,streaming_services.name,where_to_watch.streaming_type FROM series INNER JOIN where_to_watch ON series.id=where_to_watch.filmID INNER JOIN streaming_services ON " \
    #       "streaming_services.id=where_to_watch.streaming_service_id WHERE where_to_watch.type='series' AND (where_to_watch.streaming_type='ads' OR " \
    #       "where_to_watch.streaming_type='flatrate') AND series.name='The Rookie' ORDER BY where_to_watch.streaming_type DESC"
    # conn = sqlite3.connect(new_db)
    # cur = conn.cursor()
    # cur.execute(qry)
    # select = cur.fetchall()
    # for s in select:
    #     print(s)


def convert_series_movies_and_episodes_to_one_table_to_be_exported(new_db):
    fields = ['id','tmdb_id','imdb_id','title','apr_date','type','poster','budget','revenue','runtime','parent_id',
              'season_num','episode_num','overviews']
    types = ['INTEGER PRIMARY KEY','INT','TEXT','TEXT','TEXT','TEXT','TEXT','INT','INT','INT','INT','INT','INT','TEXT']
    sqliteDBProcessing.create_table('films.db','films',fields,types)
    sqliteDBProcessing.create_index('films.db','films','tmdb_id','films_tmdb_id_index')
    sqliteDBProcessing.create_index('films.db','films','imdb_id','films_imdb_id_index')
    sqliteDBProcessing.create_index('films.db','films','apr_date','films_apr_date_index')
    sqliteDBProcessing.create_index('films.db','films','type','films_type_index')
    fields.pop(0)
    types.pop(0)
    movies = sqliteDBProcessing.select_all_data(new_db,'movies')
    lngm = len(movies)
    countm = 0
    for m in movies:
        try:
            overview = sqliteDBProcessing.select_all_where(new_db,'moviesOverviews',['movieID'],[m[0]],['INT'])[0][2]
        except IndexError:
            overview = ""
        except KeyError:
            overview = ""
        tmdb_id = m[1]
        imdb_id = m[2]
        title = m[3]
        poster = m[6]
        apr_date = m[7]
        budget = m[8]
        revenue = m[9]
        runtime = m[10]
        values = [tmdb_id,imdb_id,title,apr_date,'m',poster,budget,revenue,runtime,-1,-1,-1,overview]
        sqliteDBProcessing.insert_into_table('films.db','films',fields,values,types)
        countm+=1
        print(f"inserted movie {countm} of {lngm}")
    series_old_id_new_id = {}
    series = sqliteDBProcessing.select_all_data(new_db,'series')
    lngs = len(series)
    countS = 0
    for s in series:
        tmdb_id = s[1]
        imdb_id = s[2]
        title = s[3]
        apr_date = s[6]
        poster = s[7]
        try:
            overview = sqliteDBProcessing.select_all_where(new_db,'seriesOverviews',['seriesID'],[s[0]],['INT'])[0][2]
        except IndexError:
            overview = ""
        except KeyError:
            overview = ""
        values = [tmdb_id,imdb_id,title,apr_date,'s',poster,-1,-1,-1,-1,-1,-1,overview]
        sqliteDBProcessing.insert_into_table('films.db', 'films', fields, values, types)
        result = sqliteDBProcessing.select_all_where('films.db','films',['tmdb_id','type'],[tmdb_id,'s'],['INT','TEXT'])[0][0]
        series_old_id_new_id[s[0]]=result
        countS+=1
        print(f"inserted series {countS} of {lngs}")
    episodes = sqliteDBProcessing.select_all_data(new_db,'episodes')
    lnge = len(episodes)
    counte = 0
    for e in episodes:
        try:
            tmdb_id = e[1]
            imdb_id = e[2]
            title = e[3]
            apr_date = e[4]
            type = 'e'
            poster = 'None'
            budget = -1
            revenue = -1
            runtime = e[8]
            parent_id = series_old_id_new_id[e[5]]
            season_num = e[6]
            episode_num = e[7]
            try:
                overview = sqliteDBProcessing.select_all_where(new_db,'episodesOverviews',['episodeID'],[e[0]],['INT'])[0][2]
            except IndexError:
                overview = ""
            except KeyError:
                overview = ""
            values = [tmdb_id, imdb_id, title, apr_date, type, poster, budget, revenue, runtime, parent_id, season_num,
                      episode_num, overview]
            sqliteDBProcessing.insert_into_table('films.db', 'films', fields, values, types)
        except KeyError:
            pass
        except IndexError:
            pass
        counte+=1
        print(f"inserted episode {counte} of {lnge}")


def download_all_movies(new_db):
    select = sqliteDBProcessing.select_all_data(new_db, 'movies')
    image_urls = []
    print(len(select))
    for s in select:
        if s[6][0] == '/':
            image_urls.append(s[6])
    select = sqliteDBProcessing.select_all_data(new_db, 'series')
    for s in select:
        if s[7][0] == '/':
            image_urls.append(s[7])
    with futures.ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(scrape_images, image_urls)

def scrape_images(image):
    url = f"https://www.themoviedb.org/t/p/w600_and_h900_bestv2/{image}"
    img_data = requests.get(url).content
    file = "images/"+image[1:]+'.jpg'
    with open(file,'wb') as handler:
        handler.write(img_data)
    print(f'Done with {image}')
    return

def scrape_movie_where_to_watch(item):
    url = f'https://api.themoviedb.org/3/movie/{item}/watch/providers?api_key={config.apiKey}'
    print(f'Scraped {item}')
    return json.loads(requests.get(url).content)

def get_movie_streamers(db,table):
    fields = ['id','filmID','type','streaming_service_id','streaming_type']
    types = ['INTEGER PRIMARY KEY', 'INT', 'TEXT', 'INT', 'TEXT']
    fields.pop(0)
    types.pop(0)
    movies_tmdb_id = []
    mt2i = {}
    sst2i = {}
    movies = sqliteDBProcessing.select_all_data(db,'movies')
    for m in movies:
        if m[1] not in movies_tmdb_id:
            movies_tmdb_id.append(m[1])
        mt2i[m[1]]=m[0]
    streamers_tmdb_id = []
    streamers = sqliteDBProcessing.select_all_data(db, 'streaming_services')
    for s in streamers:
        if int(s[1]) not in streamers_tmdb_id:
            streamers_tmdb_id.append(int(s[1]))
        sst2i[int(s[1])] = s[0]
    lng = len(movies_tmdb_id)
    count = 0
    with futures.ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(scrape_movie_where_to_watch,movies_tmdb_id)
    for result in results:
        try:
            item = result['id']
            data = result['results']['US']
            try:
                flat = data['flatrate']
                for f in flat:
                    if f['provider_id'] in streamers_tmdb_id:
                        values = [mt2i[item], 'movies', sst2i[int(f['provider_id'])], 'flatrate']
                        sqliteDBProcessing.insert_into_table(db, table, fields, values, types)
            except KeyError:
                pass
            try:
                ads = data['ads']
                for a in ads:
                    if a['provider_id'] in streamers_tmdb_id:
                        values = [mt2i[item], 'movies', sst2i[int(f['provider_id'])],'ads']
                        sqliteDBProcessing.insert_into_table(db,table,fields,values,types)
            except KeyError:
                pass
            try:
                rent = data['rent']
                for r in rent:
                    if r['provider_id'] in streamers_tmdb_id:
                        values = [mt2i[item],'movies',sst2i[int(r['provider_id'])], 'rent']
                        sqliteDBProcessing.insert_into_table(db, table, fields, values, types)
            except KeyError:
                pass
            try:
                buy = data['buy']
                for b in buy:
                    if b['provider_id'] in streamers_tmdb_id:
                        values = [mt2i[item],'movies',sst2i[int(b['provider_id'])], 'buy']
                        sqliteDBProcessing.insert_into_table(db,table,fields,values,types)
            except KeyError:
                pass
        except KeyError:
            pass
        count += 1
        print(f"{count} of {lng}")


def scrape_series_where_to_watch(item):
    url = f'https://api.themoviedb.org/3/tv/{item}/watch/providers?api_key={config.apiKey}'
    result = json.loads(requests.get(url).content)
    print(f"Scraped {item}")
    return result

def get_series_streamers(db,table):
    fields = ['id','filmID','type','streaming_service_id','streaming_type']
    types = ['INTEGER PRIMARY KEY','INT','TEXT','INT','TEXT']
    sqliteDBProcessing.create_table(db,table,fields,types)
    sqliteDBProcessing.create_index(db,table,'filmID','w2w_film_id_index')
    sqliteDBProcessing.create_index(db,table,'streaming_service_id','w2w_ss_id_index')
    fields.pop(0)
    types.pop(0)
    series_tmdb_id = []
    st2i={}
    sst2i = {}
    series = sqliteDBProcessing.select_all_data(db,'series')
    for s in series:
        if s[1] not in series_tmdb_id:
            series_tmdb_id.append(s[1])
        st2i[s[1]]=s[0]
    streamers_tmdb_id = []
    streamers = sqliteDBProcessing.select_all_data(db,'streaming_services')
    for s in streamers:
        if int(s[1]) not in streamers_tmdb_id:
            streamers_tmdb_id.append(int(s[1]))
        sst2i[int(s[1])]=s[0]
    lng = len(series_tmdb_id)
    count = 0
    with futures.ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(scrape_series_where_to_watch,series_tmdb_id)
    for result in results:
        try:
            item = result['id']
            data = result['results']['US']
            try:
                ads = data['ads']
                for a in ads:
                    if a['provider_id'] in streamers_tmdb_id:
                        values = [st2i[item],'series',sst2i[int(a['provider_id'])],'ads']
                        sqliteDBProcessing.insert_into_table(db,table,fields,values,types)
            except KeyError:
                pass
            try:
                flat = data['flatrate']
                for f in flat:
                    if f['provider_id'] in streamers_tmdb_id:
                        values = [st2i[item],'series',sst2i[int(f['provider_id'])],'flatrate']
                        sqliteDBProcessing.insert_into_table(db,table,fields,values,types)
            except KeyError:
                pass
            try:
                buy = data['buy']
                for b in buy:
                    if b['provider_id'] in streamers_tmdb_id:
                        values = [st2i[item],'series',sst2i[int(b['provider_id'])],'buy']
                        sqliteDBProcessing.insert_into_table(db,table,fields,values,types)
            except KeyError:
                pass
        except KeyError:
            pass
        count+=1
        print(f"{count} of {lng}")


def get_streaming_services(db):
    sqliteDBProcessing.create_table(db,'streaming_services',['id','tmdb_id','name','logo','price'],
                                    ['INTEGER PRIMARY KEY', 'TEXT', 'TEXT', 'TEXT', 'TEXT'])
    sqliteDBProcessing.create_index(db,'streaming_services','tmdb_id','streamers_tmdb_id_index')
    sqliteDBProcessing.create_index(db,'streaming_services','name','streamers_name_index')
    with open('streamers.tsv','r',encoding='utf-8') as fin:
        lines = fin.readlines()
    for line in lines:
        tmdb_id,name,hold = line.strip().split('\t')
        logo = hold[:-1].strip()
        price = hold[-1:]
        sqliteDBProcessing.insert_into_table(db,'streaming_services',['tmdb_id','name','logo','price'],
                                             [int(tmdb_id),name,logo,price],['TEXT','TEXT','TEXT','TEXT'])

def get_imdb_ids_for_episodes(db):
    episodes = sqliteDBProcessing.select_all_data(db,'episodes')
    for e in episodes:
        try:
            series = sqliteDBProcessing.select_all_where(db,'series',['id'],[e[5]],['INT'])[0][1]
            if e[5]>1063:
                url = f"https://api.themoviedb.org/3/tv/{series}/season/{e[6]}/episode/{e[7]}/external_ids?api_key={config.apiKey}"
                data = json.loads(requests.get(url).content)
                try:
                    try:
                        if 'imdb_id' in data and data['imdb_id'][:2]=='tt':
                            sqliteDBProcessing.update_where(db,'episodes',['imdb_id'],e[0],['TEXT'],[data['imdb_id']],'id')
                    except TypeError:
                        pass
                    except IndexError:
                        pass
                except KeyError:
                    pass
                except IndexError:
                    pass
        except KeyError:
            pass
        except IndexError:
            pass
        print(e)

def get_imdb_ids_for_series(db):
    series = sqliteDBProcessing.select_all_data(db,'series')
    lng = len(series)
    count=0
    for s in series:
        url = f'https://api.themoviedb.org/3/tv/{s[1]}/external_ids?api_key={config.apiKey}&language=en-US'
        imdb_id = json.loads(requests.get(url).content)['imdb_id']
        sqliteDBProcessing.update_where(db,'series',['imdb_id'],s[1],['TEXT'],[imdb_id],'tmdb_id')
        count+=1
        print(f"{count} of {lng}")

def delete_duplicate_movies(new_db):
    with open('duplicate-movies.tsv','r',encoding='utf-8') as fin:
        lines = fin.readlines()
    for line in lines:
        ls = line.strip()[1:-1].split(', ')
        tmdb_id = ls[1]
        qry = "DELETE FROM movies WHERE tmdb_id="+tmdb_id
        conn = sqlite3.connect(new_db)
        cur = conn.cursor()
        cur.execute(qry)
        conn.commit()
        conn.close()
        time.sleep(3000)
        print(line)
def delete_duplicate_series_episodes_and_casts(new_db):
    with open('duplicate-series.tsv','r',encoding='utf-8') as fin:
        lines = fin.readlines()
    for line in lines:
        ls = line.strip()[1:-1].split(', ')
        ls[0]=int(ls[0])
        used_episodes = []
        ep_t_to_id = {}
        select = sqliteDBProcessing.select_all_where(new_db,'episodes',['seriesID'],[ls[0]],['INT'])
        for s in select:
            if s[1] not in used_episodes:
                used_episodes.append(s[1])
                ep_t_to_id[s[1]] = s
            else:
                cast_select = sqliteDBProcessing.select_all_where(new_db,'casts',['parentID','filmID'],[s[5],s[0]],['INT','INT'])
                for cs in cast_select:
                    cast_qry = "DELETE FROM casts WHERE id="+str(cs[0])
                    conn = sqlite3.connect('data.db')
                    cur = conn.cursor()
                    cur.execute(cast_qry)
                    conn.commit()
                    conn.close()
                episode_qry = "DELETE FROM episodes WHERE id="+str(s[0])
                conn = sqlite3.connect('data.db')
                cur = conn.cursor()
                cur.execute(episode_qry)
                conn.commit()
                conn.close()
        series_qry = "DELETE FROM series WHERE id="+str(ls[0])
        conn = sqlite3.connect('data.db')
        cur = conn.cursor()
        cur.execute(series_qry)
        conn.commit()
        conn.close()

def find_duplicate(file, my_type):
    items = sqliteDBProcessing.select_all_data(my_type)
    tmdb_id_used = []
    for i in items:
        if i[1] not in tmdb_id_used:
            tmdb_id_used.append(i[1])
        else:
            with open(file, 'a', encoding='utf-8') as fout:
                fout.write(f"{i}\n")
                print(i)

def insert_release_dates_for_movies(db,table,fields,types):
    sqliteDBProcessing.create_table(db,table,fields,types)
    sqliteDBProcessing.create_index(db,table,'movieID','mrd_movie_id_index')
    sqliteDBProcessing.create_index(db,table,'release_date','mrd_release_date_index')
    sqliteDBProcessing.create_index(db,table,'type','mrd_type_index')
    fields.pop(0)
    types.pop(0)
    with open('movie_release_dates.tsv','r',encoding='utf-8') as fin:
        lines = fin.readlines()
    lng = len(lines)
    count=0
    for line in lines:
        ls = line.strip().split('\t')
        values = [int(ls[0]),ls[1],ls[2],int(ls[3])]
        sqliteDBProcessing.insert_into_table(db,table,fields,values,types)
        count+=1
        print(f"{count} of {lng}")


def get_all_release_dates_for_movies():
    movies = sqliteDBProcessing.select_all_data('movies')
    movie_release_date_info = []
    movies_to_search = []
    batch = []
    for m in movies:
        if len(batch) == 1000:
            movies_to_search.append(batch)
            batch = []
        batch.append(m[1])
    movies_to_search.append(batch)
    total_batches = len(movies_to_search)
    total_records = len(movies)
    movies_done = 0
    batches_done = 0
    for batch in movies_to_search:
        with futures.ThreadPoolExecutor(max_workers=100) as executor:
            results = executor.map(scrape_movie_rd, batch)
        for result in results:
            try:
                movie_id = result['id']
                my_results = result['results']
                for mr in my_results:
                    if mr['iso_3166_1'] == 'US':
                        release_dates = mr['release_dates']
                        for rd in release_dates:
                            values = []
                            values.append(movie_id)
                            values.append(rd['certification'])
                            values.append(rd['release_date'][:10])
                            values.append(rd['type'])
                            movie_release_date_info.append(values)
            except KeyError or IndexError:
                pass
            movies_done += 1
        with open('movie_release_dates.tsv', 'a', encoding='utf-8') as fout:
            for m in movie_release_date_info:
                fout.write(f'{m[0]}\t{m[1]}\t{m[2]}\t{m[3]}\n')
        batches_done += 1
        movie_release_date_info = []
        print(f'Batch {batches_done} of {total_batches} done')


def scrape_movie_rd(num):
    return json.loads(requests.get(f'https://api.themoviedb.org/3/movie/{num}/release_dates?api_key={config.apiKey}').content)

def insert_overview(db,table,fields,types,bys,indices,file):
    sqliteDBProcessing.create_table(db,table,fields,types)
    for x in range(0,len(bys)):
        sqliteDBProcessing.create_index(db,table,bys[x],indices[x])
    fields.pop(0)
    types.pop(0)
    with open(file,'r',encoding='utf-8') as fin:
        lines=fin.readlines()
    lng = len(lines)
    count=0
    for line in lines:
        try:
            my_id,overview = line.strip().split('\t')
            overview = overview.replace('\"','`').replace('"','`').replace('\'','`')
            values =[my_id,overview]
            if len(overview)>1:
                try:
                    sqliteDBProcessing.insert_into_table(db,table,fields,values,types)
                    count += 1
                    print(f"{count} of {lng} done")
                except sqlite3.OperationalError:
                    with open('sqlite-error.tsv','a',encoding='utf-8') as fout:
                        fout.write(line)
                    print(f"ERROR: {line}")
        except ValueError:
            pass

def create_movie_casts(db, new_db):
    print('Reading in actors')
    actors = sqliteDBProcessing.select_all_data(new_db,'actors')
    actors_used = {}
    for a in actors:
        actors_used[a[1]]=a[0]
    print(len(actors_used))
    print('Reading in movies')
    movies = sqliteDBProcessing.select_all_data(new_db,'movies')
    all_batches = []
    batch = []
    movie_tmdb_id_to_my_id = {}
    print('Batching movies')
    for m in movies:
        if len(batch)==1000:
            all_batches.append(batch)
            batch = []
        # if m[0]>14560:
            batch.append(m[1])
            movie_tmdb_id_to_my_id[m[1]]=m[0]
    all_batches.append(batch)
    fields = ['parentID','filmID','actorID','character']
    types = ['INT','INT','INT','TEXT']
    total_batches = len(all_batches)
    batch_complete = 0
    count=0
    for batch in all_batches:
        with futures.ThreadPoolExecutor(max_workers=100) as executor:
            results = data_collection.do_futures(scrape_movie_cast,batch)
        for result in results:
            try:
                cast = result['cast']
                for c in cast:
                    try:
                        a_t_id = c['id']
                        if a_t_id not in actors_used.keys():
                            actor_data = json.loads(requests.get(f'https://api.themoviedb.org/3/person/{a_t_id}?api_key={config.apiKey}&language=en-US').content)
                            a_fields = ['tmdb_id','imdb_id','name','gender','pob','birth','death']
                            a_types = ['INT','TEXT','TEXT','INT','TEXT','TEXT','TEXT']
                            a_values = [actor_data['id'],actor_data['imdb_id'],actor_data['name'].replace('\"','`').replace('"','`').replace('\'','`'),actor_data['gender'],actor_data['place_of_birth'],str(actor_data['birthday']),str(actor_data['deathday'])]
                            sqliteDBProcessing.insert_into_table(new_db,'actors',a_fields,a_values,a_types)
                            this_actor = sqliteDBProcessing.select_all_where(new_db,'actors',['tmdb_id'],[a_t_id],['INT'])[0][0]
                            actors_used[a_t_id]=this_actor
                        my_actor = actors_used[a_t_id]
                        character = c['character'].replace('\"','`').replace('"','`').replace('\'','`')
                        this_movie = movie_tmdb_id_to_my_id[result['id']]
                        values = [-1,this_movie,my_actor,character]
                        sqliteDBProcessing.insert_into_table(new_db,'casts',fields,values,types)
                    except KeyError or IndexError:
                        pass
            except KeyError or IndexError:
                pass
            count+=1
            print(f"{count} movies done")
        batch_complete+=1
        print(f'{batch_complete} of {total_batches}')


def scrape_movie_cast(movie_id):
    return json.loads(requests.get(f"https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={config.apiKey}&language=en-US").content)
def create_episode_casts_in_data_db(db, new_db):
    casts_fields = ['id','parentID','filmID','actorID','character']
    casts_types = ['INTEGER PRIMARY KEY','INT','INT','INT','TEXT']
    actor_fields = ['id','tmdb_id','imdb_id','name','gender','pob','birth','death']
    actor_types = ['INTEGER PRIMARY KEY','INT','TEXT','TEXT','INT','TEXT','TEXT','TEXT']
    sqliteDBProcessing.create_table(new_db,'casts',casts_fields,casts_types)
    sqliteDBProcessing.create_index(new_db,'casts','parentID','casts_parent_id_index')
    sqliteDBProcessing.create_index(new_db,'casts','filmID','casts_film_id_index')
    sqliteDBProcessing.create_index(new_db, 'casts', 'actorID', 'casts_actor_id_index')
    sqliteDBProcessing.create_index(new_db, 'casts', 'character', 'casts_character_index')
    sqliteDBProcessing.create_table(new_db,'actors',actor_fields,actor_types)
    sqliteDBProcessing.create_index(new_db,'actors','tmdb_id','actor_tmdb_id_index')
    sqliteDBProcessing.create_index(new_db,'actors','imdb_id','actor_imdb_id_index')
    sqliteDBProcessing.create_index(new_db,'actors','name','actor_name_index')
    actor_types.pop(0)
    actor_fields.pop(0)
    casts_types.pop(0)
    casts_fields.pop(0)
    episodes = sqliteDBProcessing.select_all_data(new_db,'episodes')
    actorsMade = []
    count = 0
    lng = len(episodes)
    for e in episodes:
        try:
            old_id = sqliteDBProcessing.select_all_where(db,'episodes',['tmdb_id'],[e[1]],['INT'])[0][0]
            try:
                old_casts = sqliteDBProcessing.select_all_where(db,'casts',['filmID'],[old_id],['INT'])
                if len(old_casts)>0:
                    for oc in old_casts:
                        if oc[3] not in actorsMade:
                            try:
                                a = sqliteDBProcessing.select_all_where(db,'actors',['tmdb_id'],[oc[3]],['INT'])[0]
                                actor_values = [a[1],a[2],a[3],a[4],a[5],a[6],a[7]]
                                sqliteDBProcessing.insert_into_table(new_db,'actors',actor_fields,actor_values,actor_types)
                                actorsMade.append(a[1])
                            except KeyError:
                                pass
                            except IndexError:
                                pass
                        new_actor_id = sqliteDBProcessing.select_all_where(new_db,'actors',['tmdb_id'],[oc[3]],['INT'])[0][0]
                        parent_id = e[5]
                        character = oc[4]
                        this_episode_id = e[0]
                        cast_value = [parent_id,this_episode_id,new_actor_id,character]
                        sqliteDBProcessing.insert_into_table(new_db,'casts',casts_fields,cast_value,casts_types)
            except KeyError:
                pass
            except IndexError:
                pass
        except KeyError:
            pass
        except IndexError:
            pass
        count+=1
        print(f"{count} of {lng}")

    
def create_series_g_n_pc_in_data_db(db, new_db):
    sg_fields = ['id','seriesID','genreID']
    types = ['INTEGER PRIMARY KEY','INT','INT']
    pc_fields = ['id','seriesID','prodCoID']
    net_fields = ['id','seriesID','networkID']
    sqliteDBProcessing.create_table(new_db,'seriesGenres',sg_fields,types)
    sqliteDBProcessing.create_index(new_db,'seriesGenres','seriesID','sg_series_id_index')
    sqliteDBProcessing.create_index(new_db,'seriesGenres','genreID','sg_genre_id_index')
    sqliteDBProcessing.create_table(new_db,'seriesProdCos',pc_fields,types)
    sqliteDBProcessing.create_index(new_db,'seriesProdCos','seriesID','spc_series_id_index')
    sqliteDBProcessing.create_index(new_db,'seriesProdCos','prodCoID','spc_prod_co_id_index')
    sqliteDBProcessing.create_table(new_db,'seriesNetworks',net_fields,types)
    sqliteDBProcessing.create_index(new_db, 'seriesNetworks', 'seriesID', 'sn_series_id_index')
    sqliteDBProcessing.create_index(new_db, 'seriesNetworks', 'networkID', 'sn_network_id_index')
    sg_fields.pop(0)
    pc_fields.pop(0)
    net_fields.pop(0)
    types.pop(0)
    series = sqliteDBProcessing.select_all_data(new_db,'series')
    lng = len(series)
    count=0
    for s in series:
        old_id = sqliteDBProcessing.select_all_where(db, 'series', ['tmdb_id'], [s[1]], ['INT'])[0][0]

        try:
            old_sg = sqliteDBProcessing.select_all_where(db,'seriesGenres',['seriesID'],[old_id],['INT'])
            for osg in old_sg:
                sg_values = [s[0],osg[2]]
                sqliteDBProcessing.insert_into_table(new_db,'seriesGenres',sg_fields,sg_values,types)
        except KeyError:
            pass
        except IndexError:
            pass

        try:
            old_pcs = sqliteDBProcessing.select_all_where(db,'seriesProdCos',['seriesID'],[old_id],['INT'])
            for opc in old_pcs:
                pc_values = [s[0],opc[2]]
                sqliteDBProcessing.insert_into_table(new_db,'seriesProdCos',pc_fields,pc_values,types)
        except KeyError:
            pass
        except IndexError:
            pass

        try:
            old_networks = sqliteDBProcessing.select_all_where(db,'seriesNetworks',['seriesID'],[old_id],['INT'])
            for net in old_networks:
                net_values = [s[0],net[2]]
                sqliteDBProcessing.insert_into_table(new_db, 'seriesNetworks', net_fields, net_values, types)
        except KeyError:
            pass
        except IndexError:
            pass

        count+=1
        print(f"{count} of {lng}")

def create_movie_pc_in_data_db(db, new_db):
    sqliteDBProcessing.drop_table(new_db,'movieProdCos')
    fields = ['id','movieID','pcID']
    types = ['INTEGER PRIMARY KEY','INT','INT']
    sqliteDBProcessing.create_table(new_db,'movieProdCos',fields,types)
    sqliteDBProcessing.create_index(new_db,'movieProdCos','movieID','mpc_movie_id_index')
    sqliteDBProcessing.create_index(new_db,'movieProdCos','pcID','mpc_pc_id_index')
    fields.pop(0)
    types.pop(0)
    movies = sqliteDBProcessing.select_all_data(new_db,'movies')
    lng = len(movies)
    count = 0
    for m in movies:
        try:
            old_id = sqliteDBProcessing.select_all_where(db,'movies',['tmdb_id'],[m[1]],['INT'])[0][0]
            mpcs = sqliteDBProcessing.select_all_where(db,'movieProdCos',['movieID'],[old_id],['INT'])
            for mpc in mpcs:
                values = [m[0],mpc[2]]
                sqliteDBProcessing.insert_into_table(new_db,'movieProdCos',fields,values,types)
        except KeyError:
            pass
        except IndexError:
            pass
        count+=1
        print(f"{count} of {lng}")

def create_movie_genres_in_data_db(db, new_db):
    fields = ['id','movieID','genreID']
    types = ['INTEGER PRIMARY KEY','INT','INT']
    sqliteDBProcessing.create_table(new_db,'movieGenres',fields,types)
    fields.pop(0)
    types.pop(0)
    sqliteDBProcessing.create_index(new_db,'movieGenres','movieID','mg_movie_id_index')
    sqliteDBProcessing.create_index(new_db,'movieGenres','genreID', 'mg_genre_id_index')
    new_movies = sqliteDBProcessing.select_all_data(new_db,'movies')
    lng = len(new_movies)
    count = 0
    for m in new_movies:
        try:
            old_movie_id = sqliteDBProcessing.select_all_where(db,'movies',['tmdb_id'],[m[1]],['INT'])[0][0]
            old_movie_genres = sqliteDBProcessing.select_all_where(db,'movieGenres',['movieID'],[old_movie_id],['INT'])
            for omg in old_movie_genres:
                values = [m[0],omg[2]]
                sqliteDBProcessing.insert_into_table(new_db,'movieGenres',fields,values,types)
        except KeyError:
            pass
        except IndexError:
            pass
        count+=1
        print(f"{count} of {lng}")


def create_movie_collections_in_data_db(db,new_db):
    fields = ['id','movie_id','collection_id']
    types = ['INTEGER PRIMARY KEY','INT','INT']
    sqliteDBProcessing.create_table(new_db,'movieCollections',fields,types)
    sqliteDBProcessing.create_index(new_db,'movieCollections','movie_id','mc_movie_id_index')
    sqliteDBProcessing.create_index(new_db, 'movieCollections', 'collection_id', 'mc_collection_id_index')
    fields.pop(0)
    types.pop(0)
    old_movie_collections = sqliteDBProcessing.select_all_data(db,'movieCollections')
    lng = len(old_movie_collections)
    count = 0
    for omc in old_movie_collections:
        try:
            movie_tmdb = sqliteDBProcessing.select_all_where(db,'movies',['id'],[omc[1]],['INT'])[0][1]
            new_movie_id = sqliteDBProcessing.select_all_where(new_db,'movies',['tmdb_id'],[movie_tmdb],['INT'])[0][0]
            collections_tmdb = sqliteDBProcessing.select_all_where(db,'collections',['id'],[omc[2]],['INT'])[0][1]
            new_collections_id = sqliteDBProcessing.select_all_where(new_db,'collections',['tmdb_id'],[collections_tmdb],['INT'])[0][0]
            values = [new_movie_id,new_collections_id]
            sqliteDBProcessing.insert_into_table(new_db,'movieCollections',fields,values,types)
        except KeyError:
            pass
        except IndexError:
            pass
        count+=1
        print(f"{count} of {lng}")

def create_movies_in_data_db(db, new_db):
    movies = sqliteDBProcessing.select_all_data(db,'movies')
    # 'id', 'tmdb_id', 'imdb_id', 'title', 'status', 'language', 'poster', 'release_date','budget', 'revenue', 'runtime'
    all_to_search = []
    batch = []
    count = 0
    for m in movies:
        count+=1
        if m[5]=='en':
            if len(batch)==100:
                all_to_search.append(batch)
                batch = []
            batch.append(int(m[1]))
    all_to_search.append(batch)
    us_tmdb = []
    for batch in all_to_search:
        with futures.ThreadPoolExecutor(max_workers=100) as executor:
            results = data_collection.do_futures(scrape_if_US_movie,batch)
        for result in results:
            try:
                my_pcs = result['production_companies']
                us_found = False
                for x in my_pcs:
                    if x['origin_country']=='US':
                        us_found=True
                        break
                if us_found:
                    us_tmdb.append(result['id'])
            except KeyError:
                pass
            except IndexError:
                pass
    fields = ['id', 'tmdb_id', 'imdb_id', 'title', 'status', 'language', 'poster', 'release_date','budget','revenue','runtime']
    types = ['INTEGER PRIMARY KEY','INT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','INT','INT','INT']
    sqliteDBProcessing.create_table(new_db,'movies',fields,types)
    fields.pop(0)
    types.pop(0)
    sqliteDBProcessing.create_index(new_db,'movies','tmdb_id','movie_tmdb_id_index')
    sqliteDBProcessing.create_index(new_db,'movies','title','movie_title_index')
    sqliteDBProcessing.create_index(new_db,'movies','imdb_id','movie_imdb_id_index')
    lng = len(us_tmdb)
    count=0
    for item in us_tmdb:
        old_data = sqliteDBProcessing.select_all_where(db, 'movies', ['tmdb_id'], [item], ['INT'])[0]
        values = [old_data[1], old_data[2], old_data[3], old_data[4], old_data[5], old_data[6], old_data[7],old_data[8],old_data[9],old_data[10]]
        sqliteDBProcessing.insert_into_table(new_db, 'movies', fields, values, types)
        count+=1
        print(f'done with {count} of {lng}')


def scrape_if_US_movie(movie_id):
    return json.loads(requests.get(f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={config.apiKey}&language=en-US').content)
def create_g_n_pc_and_c_in_data_db(db,new_db):
    genre_fields = ['id','tmdb_id','name']
    genre_types = ['INTEGER PRIMARY KEY','INT','TEXT']
    network_fields = ['id','tmdb_id','name','logo']
    network_types = ['INTEGER PRIMARY KEY','INT','TEXT','TEXT']
    production_company_fields = ['id','tmdb_id','name','logo']
    production_company_types = ['INTEGER PRIMARY KEY','INT','TEXT','TEXT']
    collection_fields = ['id','tmdb_id','name']
    collection_types = ['INTEGER PRIMARY KEY','INT','TEXT']
    sqliteDBProcessing.create_table(new_db,'genres',genre_fields,genre_types)
    sqliteDBProcessing.create_index(new_db,'genres','name','genre_name_index')
    sqliteDBProcessing.create_index(new_db,'genres','tmdb_id','genre_tmdb_id_index')
    sqliteDBProcessing.create_table(new_db,'networks',network_fields,network_types)
    sqliteDBProcessing.create_index(new_db, 'networks','name','network_name_index')
    sqliteDBProcessing.create_index(new_db,'networks','tmdb_id','network_tmdb_id_index')
    sqliteDBProcessing.create_table(new_db,'production_companies',production_company_fields,production_company_types)
    sqliteDBProcessing.create_index(new_db,'production_companies','name','production_company_name_index')
    sqliteDBProcessing.create_index(new_db,'production_companies','tmdb_id','production_company_tmdb_id_index')
    sqliteDBProcessing.create_table(new_db,'collections',collection_fields,collection_types)
    sqliteDBProcessing.create_index(new_db,'collections','tmdb_id','collection_tmdb_id_index')
    sqliteDBProcessing.create_index(new_db,'collections','name','collection_name_index')
    genre_types.pop(0)
    genre_fields.pop(0)
    network_types.pop(0)
    network_fields.pop(0)
    production_company_fields.pop(0)
    production_company_types.pop(0)
    collection_types.pop(0)
    collection_fields.pop(0)
    insert_with_items(db,new_db,'genres',genre_fields,genre_types,2)
    insert_with_items(db,new_db,'networks',network_fields,network_types,3)
    insert_with_items(db,new_db,'production_companies',production_company_fields,production_company_types,3)
    insert_with_items(db,new_db,'collections',collection_fields,collection_types,2)

def insert_with_items(db,new_db,table,fields,types,val_len):
    items = sqliteDBProcessing.select_all_data(db,table)
    lng = len(items)
    count = 0
    for item in items:
        values = []
        for x in range(1,val_len+1):
            values.append(item[x])
        sqliteDBProcessing.insert_into_table(new_db,table,fields,values,types)
        count+=1
        print(f"{count} of {lng} {table} done")
    print(f"{table} done")

def create_episodes_in_data_db(db,new_db):
    new_series_data = sqliteDBProcessing.select_all_data(new_db,'series')
    old_id_to_new_id = {}
    for item in new_series_data:
        temp = sqliteDBProcessing.select_all_where(db,'series',['tmdb_id'],[item[1]],['INT'])[0]
        old_id_to_new_id[temp[0]]=item[0]
    fields = ['id','tmdb_id','imdb_id','name','air_date','seriesID','seasonNum','episodeNum','runtime']
    types = ['INTEGER PRIMARY KEY','INT','TEXT','TEXT','TEXT','INT','INT','INT','INT']
    sqliteDBProcessing.create_table(new_db,'episodes',fields,types)
    fields.pop(0)
    types.pop(0)
    sqliteDBProcessing.create_index(new_db,'episodes','name','episodes_name_index')
    sqliteDBProcessing.create_index(new_db,'episodes','tmdb_id','episodes_tmdb_id_index')
    lng = len(old_id_to_new_id)
    count = 0
    for old_id,new_id in old_id_to_new_id.items():
        episodes = sqliteDBProcessing.select_all_where(db,'episodes',['seriesID'],[old_id],['INT'])
        for e in episodes:
            values = [e[1],e[2],e[3],e[4],new_id,e[6],e[7],e[8]]
            sqliteDBProcessing.insert_into_table(new_db,'episodes',fields,values,types)
        count+=1
        print(f"Done with {count} of {lng}")


def create_series_in_data_db(db,new_db):
    # SERIES
    print('reading in old db')
    all_series = sqliteDBProcessing.select_all_data(db,'series')
    are_these_US = []
    print('making array')
    for s in all_series:
        if s[11]=='Returning Series' and s[10]=='True':
            if s[8][2:-2]=='en':
                are_these_US.append(s[1])
    broken_into_batches = []
    batch = []
    for item in are_these_US:
        if len(batch)==1000:
            broken_into_batches.append(batch)
            batch=[]
        batch.append(item)
    broken_into_batches.append(batch)
    fields = ['id','tmdb_id','imdb_id','name','num_seasons','num_episodes','premiered','poster']
    types = ['INTEGER PRIMARY KEY','INT','TEXT','TEXT','INT','INT','TEXT','TEXT']
    sqliteDBProcessing.create_table(new_db,'series',fields,types)
    sqliteDBProcessing.create_index(new_db,'series','tmdb_id','series_tmdb_id_index')
    sqliteDBProcessing.create_index(new_db,'series','name','series_name_index')
    fields.pop(0)
    types.pop(0)
    us_tmdb = []
    for batch in broken_into_batches:
        with futures.ThreadPoolExecutor(max_workers=100) as executor:
            results = data_collection.do_futures(scrape_if_US,batch)
        for result in results:
            try:
                if result['origin_country'][0]=='US':
                    us_tmdb.append(result['id'])
            except KeyError:
                pass
            except IndexError:
                pass
    for item in us_tmdb:
        old_data = sqliteDBProcessing.select_all_where(db,'series',['tmdb_id'],[item],['INT'])[0]
        values = [old_data[1],old_data[2],old_data[3],old_data[4],old_data[5],old_data[6],old_data[7]]
        sqliteDBProcessing.insert_into_table(new_db,'series',fields,values,types)
        print(f'done with {item}')

def scrape_if_US(tv_id):
    return json.loads(requests.get(f'https://api.themoviedb.org/3/tv/{tv_id}?api_key={config.apiKey}&language=en-US').content)


def get_overview_movies():
    movies = sqliteDBProcessing.select_all_data('movies')
    batches = []
    batch = []
    tmdb_to_id = {}
    for m in movies:
        tmdb_to_id[m[1]]=m[0]
        if len(batch)==1000:
            batches.append(batch)
            batch = []
        batch.append(m[1])
    batches.append(batch)
    total_batches = len(batches)
    batch_count = 0
    for batch in batches:
        with futures.ThreadPoolExecutor(max_workers=100) as executor:
            results = executor.map(scrape_movie_overviews, batch)
        with open('movie_overviews.tsv','a',encoding='utf-8') as fout:
            for result in results:
                try:
                    overview = result['overview'].replace('\"','``').replace('"','``').replace('\'','`')
                    my_id = tmdb_to_id[result['id']]
                    fout.write(f"{my_id}\t{overview}\n")
                except KeyError or IndexError:
                    pass
        batch_count+=1
        print(f"Batch {batch_count} of {total_batches}")


def scrape_movie_overviews(movie_id):
    return json.loads(requests.get(f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={config.apiKey}&language=en-US').content)


def get_overview_episodes():
    print('Reading in series')
    series = sqliteDBProcessing.select_all_data('series')
    si2ti = {}
    for s in series:
        si2ti[s[0]]=s[1]
    print('Reading in episodes')
    eps = sqliteDBProcessing.select_all_data('episodes')
    eps_to_search = []
    eps_tmdb_to_id = {}
    for e in eps:
        #0. id 1. tmdb_id 2. imdb_id 3. name 4.air_date 5. seriesID 6. seasonNum 7. episodeNum 8.runtime
        series_id = si2ti[e[5]]
        temp = str(series_id)+'\t'+str(e[6])+'\t'+str(e[7])
        eps_to_search.append(temp)
        eps_tmdb_to_id[e[1]]=e[0]
    batches = []
    batch = []
    print('Batching')
    for e in eps_to_search:
        if len(batch)==1000:
            batches.append(batch)
            batch = []
        batch.append(e)
    batches.append(batch)
    batch_done = 0
    total_batches = len(batches)
    eps_done = 0
    eps_total = len(eps)
    for batch in batches:
        print(f"Scraping batch {batch_done+1}")
        with futures.ThreadPoolExecutor(max_workers=100) as executor:
            results = executor.map(scrape_episode_overviews, batch)
        with open('episodes_overview.tsv','a',encoding='utf-8') as fout:
            for result in results:
                try:
                    fout.write(f"{eps_tmdb_to_id[result['id']]}\t{result['overview']}\n")
                    eps_done+=1
                    print(f"{eps_done} eps of {eps_total}")
                except KeyError:
                    pass
        batch_done += 1
        print(f"{batch_done} of {total_batches}")

def scrape_episode_overviews(num):
    series_id,season_num,episode_num = num.split('\t')
    url = f"https://api.themoviedb.org/3/tv/{series_id}/season/{season_num}/episode/{episode_num}?api_key={config.apiKey}&language=en-US"
    return json.loads(requests.get(url).content)


def get_overview_series():
    series = sqliteDBProcessing.select_all_data('series')
    ab = []
    batch = []
    tmdb_to_id = {}
    for s in series:
        if len(batch)==100:
            ab.append(batch)
            batch = []
        batch.append(s[1])
        tmdb_to_id[s[1]]=s[0]
    ab.append(batch)
    batch_count=0
    total_batches = len(ab)
    for batch in ab:
        with futures.ThreadPoolExecutor(max_workers=100) as executor:
            results = executor.map(scrape_series_overviews, batch)
        with open('series-overviews.tsv','a',encoding='utf-8') as fout:
            for result in results:
                try:
                    overview = result['overview'].replace('\"','``').replace('"','``').replace('\'','`')
                    my_id = tmdb_to_id[result['id']]
                    fout.write(f"{my_id}\t{overview}\n")
                except KeyError or IndexError:
                    pass
        batch_count += 1
        print(f"Batch {batch_count} of {total_batches}")

def scrape_series_overviews(num):
    return json.loads(requests.get(f"https://api.themoviedb.org/3/tv/{num}?api_key={config.apiKey}&language=en-US").content)