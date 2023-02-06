import datetime
import logging
import sqlite3
import os
import shutil
import gzip
import requests
import config
import concurrent.futures as futures
import json
import time
import sqliteDBProcessing

def get_original_data(db):
    today = str(datetime.date.today())
    dir = 'json_dir'
    files = ["movie","tv_series","person","collection","tv_network","production_company"]
    my_files = []
    if not os.path.exists(dir):
        os.mkdir(dir)
    for file in files:
        this_file = f"{file}_ids_{today[5:7]}_{today[8:]}_{today[:4]}.json"
        my_files.append(this_file)
        url = f"http://files.tmdb.org/p/exports/{this_file}.gz"
        with open(f"{this_file}.gz","wb") as f:
            req = requests.get(url).content
            f.write(req)
        with gzip.open(f"{this_file}.gz",'rb') as fin:
            with open(f"{dir}/{this_file}","wb") as fout:
                shutil.copyfileobj(fin,fout)
        os.remove(f"{this_file}.gz")
        print(f"Done with {file}")
    sqliteDBProcessing.create_all_tables(db)
    read_actors(dir,my_files[2],'actors','Actors',scrape_actors)
    read_genres()
    read_simple(dir,my_files[4],'networks','Networks',scrape_networks)
    read_simple(dir,my_files[5],'production_companies','PCs',scrape_prod_cos)
    read_collections(dir,my_files[3])
    networks,prod_cos,genres = build_dictionaries()
    process_seasons_and_episodes(dir,my_files[1],scrape_series,networks,genres,prod_cos)
    process_movies(dir,my_files[0])
    process_episode_cast()
    sqliteDBProcessing.backup(db)
    convertSeriesForUSOnly()
    convertUSSeriesToCurrent()
    convertEpisodesToCurrentSeriesEpisodes()


def build_us_series_only(db1, db2):
    #get_only_current_data_for_app(db1,db2)
    get_current_episode_casts(db1,db2)

def get_current_episode_casts(db1,db2):
    try:
        sqliteDBProcessing.drop_table(db2, 'casts')
        sqliteDBProcessing.drop_table(db2, 'actors')
    except Exception:
        pass
    current_series = sqliteDBProcessing.select_all_data(db2,'current_series')
    casts_fields = ['id','seriesID','episodeID','actorID','character']
    casts_types = ['INTEGER PRIMARY KEY','INT','INT','INT','TEXT']
    casts_table_name = 'casts'
    sqliteDBProcessing.create_table(db2,casts_table_name,casts_fields,casts_types)
    casts_fields.pop(0)
    casts_types.pop(0)
    actor_fields = ['id','tmdb_id', 'imdb_id', 'name', 'gender', 'pob', 'birth', 'death']
    actor_types = ['INTEGER PRIMARY KEY', 'INT', 'TEXT', 'TEXT', 'INT', 'TEXT', 'TEXT', 'TEXT']
    actor_table_name = 'actors'
    sqliteDBProcessing.create_table(db2, actor_table_name, actor_fields, actor_types)
    actor_fields.pop(0)
    actor_types.pop(0)
    used_actors_by_tmdb_id = []
    used_actor_dict = {}
    for s in current_series:
        print(s)
        new_series_id = s[0]
        old_series_id = sqliteDBProcessing.select_all_where(db1,'series',['tmdb_id'],[s[1]],['INT'])[0][0]
        casts = sqliteDBProcessing.select_all_where(db1,'casts',['parentID'],[old_series_id],['INT'])
        for c in casts:
            print(f"{s[0]}  {c}")
            character = c[4]
            old_actor_tmdb_id = c[3]
            old_episode_id = c[2]
            old_episode = sqliteDBProcessing.select_all_where(db1,'episodes',['id'],[old_episode_id],['INT'])[0][1]
            new_episode_id = sqliteDBProcessing.select_all_where(db2,'current_episodes',['tmdb_id'],[old_episode],['INT'])[0][0]
            if old_actor_tmdb_id not in used_actors_by_tmdb_id:
                try:
                    old_actor_data = \
                    sqliteDBProcessing.select_all_where(db1, 'actors', ['tmdb_id'], [old_actor_tmdb_id], ['INT'])[0]
                    a_vals = [old_actor_tmdb_id,old_actor_data[2],old_actor_data[3],old_actor_data[4],old_actor_data[5],old_actor_data[6],old_actor_data[7]]
                    sqliteDBProcessing.insert_into_table(db2,actor_table_name,actor_fields,a_vals,actor_types)
                    used_actors_by_tmdb_id.append(old_actor_tmdb_id)
                    if old_actor_tmdb_id not in used_actor_dict.keys():
                        new_actor_id = \
                        sqliteDBProcessing.select_all_where(db2, actor_table_name, ['tmdb_id'], [old_actor_tmdb_id],
                                                            ['INT'])[0][0]
                        used_actor_dict[old_actor_tmdb_id] = new_actor_id
                    else:
                        new_actor_id = used_actor_dict[old_actor_tmdb_id]
                    vals = [new_series_id, new_episode_id, new_actor_id, character]
                    sqliteDBProcessing.insert_into_table(db2, casts_table_name, casts_fields, vals, casts_types)
                except IndexError:
                    logging.error(f'Error for {old_actor_data}')
        print(f"{s[0]} done")


def get_only_current_data_for_app(db,db2):
    all_series = sqliteDBProcessing.select_all_data(db,'series')
    current_series = []
    for s in all_series:
        if s[8][2:-2]=='en' and s[10]=='True' and s[11]=='Returning Series':
            current_series.append(s)
    series_table_name = 'current_series'
    episode_table_name = 'current_episodes'
    sqliteDBProcessing.drop_table(db2, series_table_name)
    sqliteDBProcessing.drop_table(db2, episode_table_name)
    series_fields = ['id','tmdb_id','imdb_id','name','num_seasons','num_episodes','premiered','poster']
    episode_fields = ['id','tmdb_id','imdb_id','name','air_date','seriesID','seasonNum','episodeNum','runtime']
    series_types = ['INTEGER PRIMARY KEY', 'INT','TEXT','TEXT','INT','INT','TEXT','TEXT']
    episode_types = ['INTEGER PRIMARY KEY', 'INT', 'TEXT', 'TEXT', 'TEXT', 'INT', 'INT', 'INT', 'INT']
    complete_count = 0
    lng = len(current_series)
    sqliteDBProcessing.create_table(db2,series_table_name,series_fields,series_types)
    sqliteDBProcessing.create_table(db2,episode_table_name,episode_fields,episode_types)
    sqliteDBProcessing.show_list_of_tables(db2)
    series_types.pop(0)
    series_fields.pop(0)
    episode_fields.pop(0)
    episode_types.pop(0)
    for c in current_series:
        seasons = []
        series_id = c[0]
        series_tmdb_id = c[1]
        series_imdb_id = c[2]
        if len(series_imdb_id)==0:
            series_imdb_id=' '
        series_name = c[3]
        series_premiered = c[6]
        series_poster = c[7]
        episodes = sqliteDBProcessing.select_all_where(db, 'episodes', ['seriesID'], [series_id], ['INT'])
        for e in episodes:
            if e[6] not in seasons:
                seasons.append(e[6])
        series_num_episodes = len(episodes)
        series_num_seasons = len(seasons)
        series_vals = [series_tmdb_id, series_imdb_id, series_name, series_num_seasons, series_num_episodes, series_premiered, series_poster]
        sqliteDBProcessing.insert_into_table(db2, series_table_name, series_fields, series_vals, series_types)
        select = sqliteDBProcessing.select_all_data(db2, series_table_name)
        this_series_id = sqliteDBProcessing.select_id_where(db2, series_table_name, ['tmdb_id'], [series_tmdb_id], ['INT'])[0][0]
        episodes = sqliteDBProcessing.select_all_where(db, 'episodes', ['seriesID'], [series_id], ['INT'])
        for e in episodes:
            ep_tmdb_id = e[1]
            ep_imdb_id = e[2]
            ep_name = e[3]
            ep_air_date = e[4]
            ep_season_num = e[6]
            ep_episode_num = e[7]
            ep_runtime = e[8]
            ep_vals = [ep_tmdb_id,ep_imdb_id,ep_name,ep_air_date,this_series_id,ep_season_num,ep_episode_num,ep_runtime]
            sqliteDBProcessing.insert_into_table(db2, episode_table_name, episode_fields, ep_vals, episode_types)
        complete_count+=1
        print(f"{complete_count} of {lng}")

def convertEpisodesToCurrentSeriesEpisodes(db):
    qry = "SELECT episodes.tmdb_id,episodes.imdb_id,episodes.name,episodes.air_date,episodes.seriesID," \
          "episodes.seasonNum,episodes.episodeNum,episodes.runtime,series.id,currentUSShows.id,currentUSShows.tmdb_id," \
          "currentUSShows.name FROM episodes INNER JOIN series ON episodes.seriesID=series.id INNER JOIN currentUSShows " \
          "ON series.tmdb_id=currentUSShows.tmdb_id"
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(qry)
    select = cur.fetchall()
    fields = ['tmdb_id', 'imdb_id', 'name', 'air_date', 'seriesID','seasonNum', 'episodeNum', 'runtime']
    types = ['INT','TEXT','TEXT','TEXT','INT','INT','INT','INT']
    for s in select:
        tmdb_id = s[0]
        imdb_id = s[1]
        name = s[2]
        air_date = s[3]
        seriesID = s[4]
        seasonNum =s[5]
        episodeNum = s[6]
        runtime = s[7]
        vals = [tmdb_id,imdb_id,name,air_date,seriesID,seasonNum,episodeNum,runtime]
        sqliteDBProcessing.insert_into_table('episodesForCurrentUSShows',fields,vals,types)
        print(f"inserted {s}")
    return select

def convertUSSeriesToCurrent(db):
    select = sqliteDBProcessing.select_all_data(db,'usSeries')
    for s in select:
        if s[9] == 'True' and s[10] == 'Returning Series':
            tmdb_id = s[1]
            imdb_id = s[2]
            name = s[3]
            num_seasons = s[4]
            num_episodes = s[5]
            premiered = s[6]
            poster = s[7]
            languages = s[8]
            in_production = s[9]
            status = s[10]
            values = [tmdb_id, imdb_id, name, num_seasons, num_episodes, premiered, poster, languages, in_production,
                      status]
            sqliteDBProcessing.insert_into_table(db,'currentUSShows', ['tmdb_id', 'imdb_id', 'name', 'num_seasons',
                                                                    'num_episodes', 'premiered', 'poster', 'languages',
                                                                    'in_production', 'status'], values,
                                                 ['INT', 'TEXT', 'TEXT', 'INT', 'INT', 'TEXT', 'TEXT', 'TEXT', 'TEXT',
                                                  'TEXT'])

def convertSeriesForUSOnly(db):
    select = sqliteDBProcessing.select_all_data(db,'series')
    for s in select:
        if s[9] == 1:
            tmdb_id = s[1]
            imdb_id = s[2]
            name = s[3]
            num_seasons = s[4]
            num_episodes = s[5]
            premiered = s[6]
            poster = s[7]
            languages = s[8]
            in_production = s[10]
            status = s[11]
            values = [tmdb_id, imdb_id, name, num_seasons, num_episodes, premiered, poster, languages, in_production,
                      status]
            sqliteDBProcessing.insert_into_table('usSeries',
                                                 ['tmdb_id', 'imdb_id', 'name', 'num_seasons', 'num_episodes',
                                                  'premiered', 'poster', 'languages', 'in_production', 'status'],
                                                 values,
                                                 ['INT', 'TEXT', 'TEXT', 'INT', 'INT', 'TEXT', 'TEXT', 'TEXT', 'TEXT',
                                                  'TEXT'])
        print(s[0])

def process_episode_cast(db):
    print('reading in episodes')
    select = sqliteDBProcessing.select_all_data(db,'episodes')
    print(len(select))
    my_series = {}
    print('reading in series')
    seriesData = sqliteDBProcessing.select_all_data(db,'series')
    print('reading in actors')
    actorData = sqliteDBProcessing.select_all_data(db,'actors')
    my_actors = {}
    print('iterating actors')
    for a in actorData:
        my_actors[a[1]]=a[0]
    print('iterating series')
    for s in seriesData:
        my_series[s[0]] = s[1]
    print('Ready to add to episodeCast')
    BATCH_SIZE = 5000
    all_Eps = []
    batch = []
    for s in select:
        if len(batch)==BATCH_SIZE:
            all_Eps.append(batch)
            batch = []
        if s[0]>981979:
            seriesTMDBID = my_series[s[5]]
            seasonNum = s[6]
            episodeNum = s[7]
            epID = s[0]
            temp = str(epID) + '\t' + str(s[5]) + '\t' + str(seriesTMDBID) + '\t' + str(seasonNum) + '\t' + str(episodeNum)
            batch.append(temp)
    all_Eps.append(batch)

    for batch in all_Eps:
        results = do_futures(scrape_ep_cast,batch)
        for result in results:
            if len(result)>0:
                try:
                    seriesID,epID,all_people = result.split('\t')
                    seriesID = int(seriesID)
                    epID = int(epID)
                    people = all_people.split('[[[~~~]]]')
                    for person in people:
                        actorID,character = person.split('(((~~~)))')
                        actorID = int(actorID)
                        sqliteDBProcessing.insert_into_table('casts',['parentID','filmID','actorID','character'],
                                                             [int(seriesID),int(epID),int(actorID),character],
                                                             ['INT','INT','INT','TEXT'])
                        print('Inserted ',seriesID, '\t', epID, '\t', actorID, '\t', character)
                except ValueError:
                    pass

def scrape_ep_cast(db,item):
    epID,seriesID,seriesTMDBID,seasonNum,episodeNum = item.split('\t')
    url = f'https://api.themoviedb.org/3/tv/{seriesTMDBID}/season/{seasonNum}/episode/{episodeNum}/credits?api_key={config.apiKey}&language=en-US'
    try:
        data = json.loads(requests.get(url).content)
        this_cast = data['cast']
        guests = data['guest_stars']
        tmp = ''
        for c in range(0,len(this_cast)):
            actor_id = data['cast'][c]['id']
            character = data['cast'][c]['character'].replace('\'','`').replace('\"','`').replace('"','`')
            if character == '' or character == 'None' or character is None:
                character = 'Unknown'
            if len(tmp)>0:
                tmp+="[[[~~~]]]"
            tmp = tmp + str(actor_id) + '(((~~~)))' + str(character)
        for g in guests:
            actor_id = g['id']
            character = g['character'].replace('\'','`').replace('\"','`').replace('"','`')
            if character == '' or character == 'None' or character is None:
                character = 'Unknown'
            if len(tmp)>0:
                tmp+="[[[~~~]]]"
            tmp = tmp + str(actor_id)+'(((~~~)))'+str(character)
        if len(tmp)>0:
            tmp = str(seriesID)+'\t'+str(epID)+'\t'+tmp
        print(f"{epID} of 3616179 scraped")
        return tmp
    except KeyError:
        return ''

def build_dictionaries():
    networks = build_dictionary('networks')
    prod_cos = build_dictionary('production_companies')
    genres = build_dictionary('genres')
    return networks,prod_cos,genres

def build_dictionary(table):
    my_dict = {}
    print(f'Reading from {table}')
    select = sqliteDBProcessing.select_all_data(table)
    for item in select:
        my_dict[int(item[1])]=item[0]
    return my_dict


def process_movies(dir,file):
    movie_fields = ['tmdb_id', 'imdb_id', 'title', 'status', 'language', 'poster', 'release_date', 'budget',
                    'revenue',
                    'runtime']
    movie_types = ['INT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'INT', 'INT', 'INT']
    mc_fields = ['movieID', 'collectionID']
    mc_types = ['INT', 'INT']
    g_fields = ['movieID', 'genreID']
    g_types = ['INT', 'INT']
    pc_fields = ['movieID', 'pcID']
    pc_types = ['INT', 'INT']
    read_movies(dir, file, scrape_movies, movie_fields, movie_types, mc_fields, mc_types, g_fields, g_types, pc_fields,
                pc_types)

def process_seasons_and_episodes(dir,file,method,networks,genres,prod_cos):
    results = read_data(dir, file, method)
    for result in results:
        if 'id' in result:
            if result['number_of_episodes'] is not None:
                print(result)
                try:
                    tmdb_id = result['id']
                    imdb_id = ''
                    name = result['name'].replace('\'','`').replace('\"','`').replace('"','`')
                    num_seasons = result['number_of_seasons']
                    num_episodes = result['number_of_episodes']
                    premiered = result['first_air_date']
                    poster = result['poster_path']
                    languages = result['languages']
                    oc = result['origin_country']
                    isUS = False
                    for a in range(0,len(oc)):
                        if oc[a]=='US':
                            isUS = True
                    in_production = result['in_production']
                    status = result['status']
                    these_fields = ['tmdb_id', 'imdb_id', 'name', 'num_seasons', 'num_episodes', 'premiered', 'poster',
                              'languages', 'USTitle', 'in_production', 'status']
                    these_types =  ['INT', 'TEXT', 'TEXT', 'INT', 'INT', 'TEXT', 'TEXT', 'TEXT', 'INT','TEXT', 'TEXT']
                    if isUS:
                        these_values = [tmdb_id,imdb_id,name,num_seasons,num_episodes,premiered,poster,
                                        languages,1,in_production,status]
                    else:
                        these_values = [tmdb_id, imdb_id, name, num_seasons, num_episodes, premiered, poster,
                                        languages, 0, in_production, status]
                    sqliteDBProcessing.insert_into_table('series',these_fields,these_values,these_types)
                    this_series = sqliteDBProcessing.select_all_where('series',['tmdb_id'],[tmdb_id],['INT'])[0][0]
                    all_seasons = result['seasons']
                    all_genres = result['genres']
                    all_networks = result['networks']
                    all_pcs = result['production_companies']
                    for a in range(0,len(all_pcs)):
                        try:
                            this_pc = prod_cos[all_pcs[a]['id']]
                            sqliteDBProcessing.insert_into_table('seriesProdCos',['seriesID', 'prodCoID'],
                                                                 [this_series,this_pc],['INT','INT'])
                        except KeyError:
                            pass
                    for a in range(0,len(all_networks)):
                        try:
                            this_network = networks[all_networks[a]['id']]
                            sqliteDBProcessing.insert_into_table('seriesNetworks',['seriesID', 'networkID'],
                                                                 [this_series,this_network],['INT','INT'])
                        except KeyError:
                            pass
                    for a in range(0,len(all_genres)):
                        try:
                            this_genre= genres[all_genres[a]['id']]
                            sqliteDBProcessing.insert_into_table('seriesGenres',['seriesID', 'genreID'],
                                                                 [this_series,this_genre],['INT','INT'])
                        except KeyError:
                            pass
                    print(f'PC,Network,Genre for {tmdb_id} done')
                    seasons_to_scrape = []
                    for a in range(0,len(all_seasons)):
                        temp = f"{tmdb_id}\t{all_seasons[a]['season_number']}"
                        seasons_to_scrape.append(temp)
                    serSeasonResults = do_futures(scrape_episodes,seasons_to_scrape)
                    ep_fields = ['tmdb_id', 'imdb_id', 'name', 'air_date', 'seriesID', 'seasonNum', 'episodeNum', 'runtime']
                    ep_types = ['INT', 'TEXT', 'TEXT', 'TEXT', 'INT', 'INT', 'INT', 'INT']
                    for a in serSeasonResults:
                        try:
                            episodes = a['episodes']
                            for ep in episodes:
                                ep_tmdb_id = ep['id']
                                ep_imdb_id = ''
                                ep_name = ep['name'].replace('\"','`').replace('"','`').replace('\'','`')
                                ep_air_date = ep['air_date']
                                ep_num = ep['episode_number']
                                ep_runtime = ep['runtime']
                                if ep_runtime is None:
                                    ep_runtime = 0
                                ep_season_number = ep['season_number']
                                ep_values = [ep_tmdb_id,ep_imdb_id,ep_name,ep_air_date,this_series,ep_season_number,ep_num,ep_runtime]
                                sqliteDBProcessing.insert_into_table('episodes',ep_fields,ep_values,ep_types)
                        except KeyError:
                            pass
                    print(f'Completed {tmdb_id}')

                except AttributeError:
                    pass


def scrape_episodes(item):
    series,season = item.split('\t')
    url = f'https://api.themoviedb.org/3/tv/{series}/season/{season}?api_key={config.apiKey}&language=en-US'
    return json.loads(requests.get(url).content)


def read_data(dir,file,method):
    lines = read_lines(dir,file)
    myArray = get_ids(lines)
    results = do_futures(method,myArray)
    return results

def scrape_series(item):
    url = f'https://api.themoviedb.org/3/tv/{item}?api_key={config.apiKey}&language=en-US'
    print(f'{item} series scraped')
    return json.loads(requests.get(url).content)

def read_lines(dir,file):
    with open(f"{dir}/{file}",'r',encoding='utf-8') as fin:
        lines=fin.readlines()
    return lines

def get_ids(lines):
    ids = []
    for line in lines:
        if len(line)>0:
            line = json.loads(line)
            ids.append(line['id'])
    ids.sort()
    return ids

def read_genres():
    urls = [f"https://api.themoviedb.org/3/genre/movie/list?api_key={config.apiKey}&language=en-US",
            f"https://api.themoviedb.org/3/genre/tv/list?api_key={config.apiKey}&language=en-US"]
    my_genres = {}
    for url in urls:
        data = json.loads(requests.get(url).content)['genres']
        for d in data:
            if d['id'] not in my_genres.keys():
                my_genres[d['id']]=d['name']
    for k,v in my_genres.items():
        sqliteDBProcessing.insert_into_table('genres',['tmdb_id','name'],[k,v],['INT','TEXT'])
    print('Genres complete')

def read_actors(dir,file,method):
    results = read_data(dir,file,method)
    fields = ['tmdb_id','imdb_id','name','gender','pob','birth','death']
    types = ['INT','TEXT','TEXT','INT','TEXT','TEXT','TEXT']
    for result in results:
        if len(result)>0:
            if 'id' in result:
                try:
                    tmdb_id = result['id']
                    imdb_id = result['imdb_id']
                    name = result['name'].replace('\"','`').replace('"','`').replace('\'','`')
                    gender = result['gender']
                    pob = result['place_of_birth']
                    birthday = result['birthday']
                    deathday = result['deathday']
                    values = [tmdb_id,imdb_id,name,gender,pob,birthday,deathday]
                    sqliteDBProcessing.insert_into_table('actors',fields,values,types)
                    print(f'Actor {tmdb_id} done')
                except AttributeError:
                    pass

def read_simple(dir,file,table,my_type,method):
    results = read_data(dir,file,method)
    for result in results:
        if 'id' in result:
            try:
                tmdb_id = result['id']
                name = result['name'].replace('\"','\'').replace('"','\'')
                logo = result['logo_path']
                sqliteDBProcessing.insert_into_table(table,['tmdb_id','name','logo'],[tmdb_id,name,logo],['INT','TEXT','TEXT'])
                print(f"{tmdb_id} inserted into {my_type}")
            except AttributeError:
                pass

# def read_production_companies(dir,file):
#     results = read_data(dir,file,scrape_prod_cos)
#     for result in results:
#         if 'id' in result:
#             try:
#                 tmdb_id = result['id']
#                 name = result['name'].replace('\"','\'').replace('"','\'')
#                 logo = result['logo_path']
#                 sqliteDBProcessing.insert_into_table('production_companies',['tmdb_id','name','logo'],
#                                                      [tmdb_id,name,logo],['INT','TEXT','TEXT'])
#                 print(f"{result['id']} inserted into collections table")
#             except AttributeError:
#                 pass

def read_collections(dir,file):
    results = read_data(dir,file,scrape_collections)
    for result in results:
        if 'id' in result:
            try:
                tmdb_id = result['id']
                name = result['name'].replace('\"','\'').replace('"','\'')
                sqliteDBProcessing.insert_into_table('collections',['tmdb_id','name'],[tmdb_id,name],['INT','TEXT'])
                print(f"{tmdb_id} inserted into collections table")
            except AttributeError:
                pass

def read_movies(dir,file,method,fields,types,mc_fields,mc_types,g_fields,g_types,pc_fields,pc_types):
    collections = build_dictionary('collections')
    genreDict = build_dictionary('genres')
    pcDict = build_dictionary('production_companies')
    results = read_data(dir, file, method)
    for result in results:
        if len(result) > 0 and 'id' in result:
            tmdb_id = result['id']
            imdb_id = result['imdb_id']
            title = result['title'].replace('\"','`').replace('"','`').replace('\'','`')
            status = result['status']
            language = result['original_language']
            poster = result['poster_path']
            release_date = result['release_date']
            budget = result['budget']
            revenue = result['revenue']
            runtime = result['runtime']
            values = [tmdb_id,imdb_id,title,status,language,poster,release_date,budget,revenue,runtime]
            sqliteDBProcessing.insert_into_table('movies',fields,values,types)
            select = sqliteDBProcessing.select_all_where('movies',['tmdb_id'],[tmdb_id],['INT'])[0][0]
            if 'belongs_to_collection' in result:
                if result['belongs_to_collection'] is None:
                    pass
                elif isinstance(result['belongs_to_collection']['id'],int):
                    try:
                        my_collection = result['belongs_to_collection']['id']
                        my_collection = collections[my_collection]
                        sqliteDBProcessing.insert_into_table('movieCollections',mc_fields,[select,my_collection],mc_types)
                    except KeyError:
                        pass
                else:
                    print(result['belongs_to_collection'])
                    time.sleep(3000)
            genres = result['genres']
            for g in genres:
                gid = g['id']
                my_genre = genreDict[gid]
                sqliteDBProcessing.insert_into_table('movieGenres',g_fields,[select,my_genre],g_types)
            prodCos = result['production_companies']
            for pc in prodCos:
                try:
                    pcid = pc['id']
                    my_pc = pcDict[pcid]
                    sqliteDBProcessing.insert_into_table('movieProdCos',pc_fields,[select,my_pc],pc_types)
                except KeyError:
                    pass
            print(f'Complete {tmdb_id}')


def do_futures(method,items):
    print(f'Reading futures for {method}')
    with futures.ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(method,items)
    print(f'Futures done for {method}')
    return results

def scrape_actors(item):
    url = f"https://api.themoviedb.org/3/person/{item}?api_key={config.apiKey}&language=en-US"
    print(f'{item} scraped for actor')
    return json.loads(requests.get(url).content)


def scrape_networks(item):
    url = f"https://api.themoviedb.org/3/network/{item}?api_key={config.apiKey}"
    print(f'{item} scraped for network')
    return json.loads(requests.get(url).content)

def scrape_prod_cos(item):
    url = f"https://api.themoviedb.org/3/company/{item}?api_key={config.apiKey}"
    print(f"{item} scraped for PC")
    return json.loads(requests.get(url).content)

def scrape_collections(item):
    url = f"https://api.themoviedb.org/3/collection/{item}?api_key={config.apiKey}&language=en-US"
    print(f"{item} scraped")
    return json.loads(requests.get(url).content)

def scrape_movies(item):
    url = f"https://api.themoviedb.org/3/movie/{item}?api_key={config.apiKey}&language=en-US"
    print(f'{item} scraped for movie')
    return json.loads(requests.get(url).content)

def add_imdb_id_to_series(db):
    select = sqliteDBProcessing.select_all_data(db,'series')
    for s in select:
        url = f'https://api.themoviedb.org/3/tv/{s[1]}/external_ids?api_key={config.apiKey}&language=en-US'
        data = json.loads(requests.get(url).content)
        try:
            if data['imdb_id'][:2] == 'tt':
                sqliteDBProcessing.update_where('series', ['imdb_id'], s[1], ['TEXT'], [data['imdb_id']], 'tmdb_id')
                print(f'updated {s[1]}')
        except TypeError:
            pass