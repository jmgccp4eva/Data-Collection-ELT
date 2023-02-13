import sqlite3
import time


# Creates table
def create_table(db,table_name, field_array, type_array):
    query = "CREATE TABLE IF NOT EXISTS {}(".format(table_name)
    temp = ''
    for x in range(0, len(field_array)):
        if len(temp) > 0:
            temp += ", "
        temp = temp + field_array[x] + " " + type_array[x]
    temp += ")"
    query += temp
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    conn.close()

# Drops table and data completely
def drop_table(db,table_name):
    query = "DROP TABLE {}".format(table_name)
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    conn.close()

# Inserts data into supplied table
def insert_into_table(db,table_name, fields, values, types):
    temp = "("
    for a in range(0, len(fields)):
        if a > 0:
            temp += ","
        temp += fields[a]
    temp += ") VALUES ("
    for a in range(0, len(values)):
        if a > 0:
            temp += ","
        if types[a] == "TEXT":
            temp += "\""
        temp += str(values[a])
        if types[a] == "TEXT":
            temp += "\""
    temp += ")"
    query = "INSERT INTO {}{}".format(table_name, temp)
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    conn.close()

def delete_from(db,table,fields,values,types):
    qry = f"DELETE FROM {table} WHERE "
    for x in range(0,len(fields)):
        if x>0:
            qry+=" AND "
        if types[x]=='TEXT':
            qry += f"'{fields[x]}'={values[x]}"
        else:
            qry += f"{fields[x]}={values[x]}"
    print(qry)
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(qry)
    conn.commit()
    conn.close()

# Creates an index
def create_index(db,table,column,index_name):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(f"""CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column});""")
    conn.commit()
    conn.close()
    print(f'Index {index_name} created')


def select_what_from_where(db,table,fields,values,types,what):
    temp = "SELECT {} FROM {} WHERE".format(what,table)
    for a in range(0, len(fields)):
        if a > 0:
            temp += " AND "
        temp += fields[a]
        temp += "="
        if types[a] == 'TEXT':
            temp += '"'
        temp += str(values[a])
        if types[a] == 'TEXT':
            temp += '"'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(temp)
    return cur.fetchall()

# Selects all data where a condition is true
def select_id_where(db,table_name, fields, values, types):
    temp = "SELECT id FROM {} WHERE ".format(table_name)
    for a in range(0, len(fields)):
        if a > 0:
            temp += " AND "
        temp += fields[a]
        temp += "="
        if types[a] == 'TEXT':
            temp += '"'
        temp += str(values[a])
        if types[a] == 'TEXT':
            temp += '"'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(temp)
    return cur.fetchall()


# Selects all data from a particular table
def select_id_from_table(db,table_name):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("""SELECT id FROM {}""".format(table_name))
    selection = cur.fetchall()
    conn.commit()
    conn.close()
    return selection


# Selects tmdb_id from a particular table
def select_tmdb_id_from_table(db,table_name):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("""SELECT tmdb_id FROM {}""".format(table_name))
    selection = cur.fetchall()
    conn.commit()
    conn.close()
    return selection

def select_all_data(db,table_name):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("""SELECT * FROM {}""".format(table_name))
    select = cur.fetchall()
    return select

def select_all_where(db,table_name,field,value,types):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    query = "SELECT * FROM {} WHERE ".format(table_name)
    for t in range(len(types)):
        if t>0:
            query += " AND "
        if types[t]=='TEXT':
            query += f"{field[t]}='{value[t]}'"
        else:
            query += f"{field[t]}={value[t]}"
    cur.execute(query)
    select = cur.fetchall()
    return select

# Shows list of all tables
def show_list_of_tables(db):
    conn = sqlite3.connect(db)
    sql_query = """SELECT name FROM sqlite_master  
      WHERE type='table';"""
    cur = conn.cursor()
    cur.execute(sql_query)
    print(cur.fetchall())
    
def create_all_tables(db):
    create_table(db,'series',['id','tmdb_id','imdb_id','name','num_seasons','num_episodes','premiered','poster',
                           'languages','USTitle','in_production','status'],
                 ['INTEGER PRIMARY KEY','INT','TEXT','TEXT','INT','INT','TEXT','TEXT','TEXT','INT','TEXT','TEXT'])
    create_index(db,'series','tmdb_id','seriesTMDBIDIndex')
    create_index(db,'series','imdb_id','seriesIMDBIDIndex')
    create_index(db,'series','name','seriesNameIndex')
    create_index(db,'series','premiered','seriesPremieredIndex')
    create_index(db,'series','num_seasons','seriesNumSeasonsIndex')
    create_index(db,'series','num_episodes','seriesNumEpsIndex')
    create_table(db,'actors',['id','tmdb_id', 'imdb_id', 'name', 'gender', 'pob', 'birth', 'death'],
                 ['INTEGER PRIMARY KEY','INT', 'TEXT', 'TEXT', 'INT', 'TEXT', 'TEXT', 'TEXT'])
    create_index(db,'actors','tmdb_id','actorsTMDBIndex')
    create_index(db,'actors','imdb_id','actorsIMDBIDIndex')
    create_index(db,'actors','name','actorsNameIndex')
    create_table(db,'genres',['id','tmdb_id','name'],['INTEGER PRIMARY KEY','INT','TEXT'])
    create_index(db,'genres','tmdb_id','genresTMDBIDIndex')
    create_index(db,'genres','name','genresNameIndex')
    create_table(db,'networks',['id','tmdb_id','name','logo'],['INTEGER PRIMARY KEY','INT','TEXT','TEXT'])
    create_index(db,'networks','tmdb_id','networkTMDBIDIndex')
    create_index(db,'networks','name','networkNameIndex')
    create_table(db,'collections',['id','tmdb_id','name'],['INTEGER PRIMARY KEY','INT','TEXT'])
    create_index(db,'collections','tmdb_id','collectionTMDBIDIndex')
    create_index(db,'collections','name','collectionNameIndex')
    create_table(db,'production_companies',['id','tmdb_id','name','logo'],['INTEGER PRIMARY KEY','INT','TEXT','TEXT'])
    create_index(db,'production_companies','tmdb_id','productionCompanyTMDBIDIndex')
    create_index(db,'production_companies','name','producationCompanyNameIndex')
    create_table(db,'seriesGenres', ['id', 'seriesID', 'genreID', 'FOREIGN KEY(seriesID)', 'FOREIGN KEY(genreID)'],
                 ['INTEGER PRIMARY KEY', 'INT', 'INT', 'REFERENCES series(id)', 'REFERENCES genres(id)'])
    create_index(db,'seriesGenres', 'seriesID', 'sg_seriesIDIndex')
    create_index(db,'seriesGenres', 'genreID', 'sg_genreIDIndex')
    create_table(db,'seriesProdCos', ['id', 'seriesID', 'prodCoID', 'FOREIGN KEY(seriesID)', 'FOREIGN KEY(prodCoID)'],
                 ['INTEGER PRIMARY KEY', 'INT', 'INT', 'REFERENCES series(id)', 'REFERENCES production_companies(id)'])
    create_index(db,'seriesProdCos', 'seriesID', 'spc_seriesIDIndex')
    create_index(db,'seriesProdCos', 'prodCoID', 'spc_prodCoIndex')
    create_table(db,'seriesNetworks', ['id', 'seriesID', 'networkID', 'FOREIGN KEY(seriesID)', 'FOREIGN KEY(networkID)'],
                 ['INTEGER PRIMARY KEY', 'INT', 'INT', 'REFERENCES series(id)', 'REFERENCES networks(id)'])
    create_index(db,'seriesNetworks', 'seriesID', 'sn_seriesIDIndex')
    create_index(db,'seriesNetworks', 'networkID', 'sn_networkIDIndex')
    create_table(db,'episodes',['id','tmdb_id','imdb_id','name','air_date','seriesID','seasonNum','episodeNum','runtime','FOREIGN KEY(seriesID)'],
                 ['INTEGER PRIMARY KEY','INT','TEXT','TEXT','TEXT','INT','INT','INT','INT','REFERENCES series(id)'])
    create_index(db,'episodes','tmdb_id','ep_tmdbIDIndex')
    create_index(db,'episodes','imdb_id','ep_imdbIDIndex')
    create_index(db,'episodes','air_date','ep_airdateIndex')
    create_index(db,'episodes','seriesID','ep_seriesIDIndex')
    create_table(db,'movies',['id', 'tmdb_id', 'imdb_id', 'title', 'status', 'language', 'poster', 'release_date',
                            'budget','revenue','runtime'],['INTEGER PRIMARY KEY', 'INT', 'TEXT', 'TEXT', 'TEXT',
                                                           'TEXT', 'TEXT', 'TEXT', 'INT', 'INT', 'INT'])
    create_index(db,'movies', 'tmdb_id', 'movie_tmdbIDIndex')
    create_index(db,'movies', 'imdb_id', 'movie_imdbIDIndex')
    create_index(db,'movies', 'title', 'movie_titleIndex')
    create_index(db,'movies', 'language', 'movie_languageIndex')
    create_index(db,'movies', 'release_date', 'movie_releaseDateIndex')
    create_table(db,'movieCollections',['id', 'movieID', 'collectionID', 'FOREIGN KEY(movieID)',
                                     'FOREIGN KEY(collectionID)'],['INTEGER PRIMARY KEY', 'INT', 'INT',
                                                                   'REFERENCES movies(id)', 'REFERENCES collections(id)'])
    create_index(db,'movieCollections', 'movieID', 'mc_movieIDIndex')
    create_index(db,'movieCollections', 'collectionID', 'mc_collectionIDIndex')
    create_table(db,'movieGenres', ['id', 'movieID', 'genreID', 'FOREIGN KEY(movieID)', 'FOREIGN KEY(genreID)'],
                 ['INTEGER PRIMARY KEY', 'INT', 'INT', 'REFERENCES movies(id)', 'REFERENCES genres(id)'])
    create_index(db,'movieGenres', 'movieID', 'mg_movieIDIndex')
    create_index(db,'movieGenres', 'genreID', 'mg_genreIDIndex')
    create_table(db,'movieProdCos', ['id', 'movieID', 'pcID', 'FOREIGN KEY(movieID)', 'FOREIGN KEY(pcID)'],
                 ['INTEGER PRIMARY KEY', 'INT', 'INT', 'REFERENCES movies(id)', 'REFERENCES production_companies(id)'])
    create_index(db,'movieProdCos', 'movieID', 'mpc_movieIDIndex')
    create_index(db,'movieProdCos', 'pcID', 'mpc_pcIDIndex')
    create_table(db,'casts',['id','parentID','filmID','actorID','character'],['INTEGER PRIMARY KEY','INT','INT','INT',
                                                                           'TEXT'])
    create_index(db,'casts','parentID','cast_parentIDIndex')
    create_index(db,'casts','filmID','cast_filmIDIndex')
    create_index(db,'casts','actorID','cast_actorIDIndex')
    create_table(db,'usSeries',['id', 'tmdb_id', 'imdb_id', 'name', 'num_seasons', 'num_episodes', 'premiered','poster',
                             'languages', 'in_production', 'status'],['INTEGER PRIMARY KEY', 'INT', 'TEXT', 'TEXT',
                                                                      'INT', 'INT', 'TEXT', 'TEXT', 'TEXT','TEXT',
                                                                      'TEXT'])
    create_index(db,'usSeries', 'tmdb_id', 'usSeriesTMDBIDIndex')
    create_index(db,'usSeries', 'imdb_id', 'usSeriesIMDBIDIndex')
    create_index(db,'usSeries', 'name', 'usSeriesNameIndex')
    create_table(db,'currentUSShows',['id', 'tmdb_id', 'imdb_id', 'name', 'num_seasons', 'num_episodes','premiered',
                                   'poster', 'languages', 'in_production', 'status'],['INTEGER PRIMARY KEY', 'INT',
                                                                                      'TEXT', 'TEXT', 'INT', 'INT',
                                                                                      'TEXT', 'TEXT','TEXT', 'TEXT',
                                                                                      'TEXT'])
    create_index(db,'currentUSShows', 'tmdb_id', 'currentShowsTMDBIDIndex')
    create_index(db,'currentUSShows', 'imdb_id', 'currentShowsIMDBIDIndex')
    create_index(db,'currentUSShows', 'name', 'currentShowsNameIndex')
    create_table(db,'episodesForCurrentUSShows',
                                    ['id', 'tmdb_id', 'imdb_id', 'name', 'air_date', 'seriesID',
                                     'seasonNum', 'episodeNum', 'runtime',
                                     'FOREIGN KEY(seriesID)'],
                                    ['INTEGER PRIMARY KEY', 'INT', 'TEXT', 'TEXT', 'TEXT', 'INT', 'INT', 'INT', 'INT',
                                     'REFERENCES series(id)'])
    create_index(db,'episodesForCurrentUSShows', 'tmdb_id', 'epCurrent_tmdbIDIndex')
    create_index(db,'episodesForCurrentUSShows', 'imdb_id', 'epCurrent_imdbIDIndex')
    create_index(db,'episodesForCurrentUSShows', 'air_date', 'epCurrent_airdateIndex')
    create_index(db,'episodesForCurrentUSShows', 'seriesID', 'epCurrent_seriesIDIndex')

def select_with_inner_join_where(db,selectWhat,tables,onWhat1,onWhat2,whereItems,whereValues,whereTypes):
    temp = ''
    for item in selectWhat:
        if len(temp)>0:
            temp+=','
        if len(temp)==0:
            temp = "SELECT "
        temp+=item
    temp += " FROM "
    for a in range(0,len(tables)):
        if a==0:
            temp += tables[a]
        else:
            first = tables[a-1] + "." + onWhat1[a-1] + "=" + tables[a] + "." + onWhat2[a-1]

            temp = f"{temp} INNER JOIN {tables[a]} ON {first}"
    newTemp = ''
    for a in range(0,len(whereItems)):
        if len(newTemp)>0:
            newTemp+=" AND "
        else:
            newTemp+=" WHERE "
        if whereTypes[a]=='TEXT':
            newTemp+=f"{whereItems[a]}='{whereValues[a]}'"
        else:
            newTemp += f"{whereItems[a]}={whereValues[a]}"
    temp+=newTemp
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(temp)
    select = cur.fetchall()
    return select
# ['*'],['episodes','series','currentUSShows'],['seriesID',],['id']
def update_to_current_show_episodes(db,):
    # "SELECT * FROM episodes INNER JOIN series ON episodes.seriesID = series.id INNER JOIN currentUSShows ON currentUSShows."
    currentSeries = select_all_where(db,'currentUSShows',['name'],['The Rookie'],['TEXT'])
    all_episodes = select_all_data(db,'episodes')
    select = select_all_where(db,'episodes',['name','air_date'],['The Naked and the Dead','2023-01-10'],['TEXT','TEXT'])
    serSelect = select_all_where(db,'series',['id'],[71581],['INT'])
    print('Current Series Length')
    print(len(currentSeries))
    print('\nAll Episodes length')
    print(len(all_episodes))
    print('\nEpisode data')
    print(select)
    print('\nSeries data')
    print(serSelect)
    print()
    print('Current')
    print(currentSeries)
    time.sleep(3000)

# Updates record in particular table when condition is true
def update_where(db,table_name, fields, value, types, new_values, where_field):
    temp = "UPDATE {} SET ".format(table_name)
    for a in range(0, len(fields)):
        if a > 0:
            temp += " AND "
        temp += fields[a]
        temp += "="
        if types[a] == 'TEXT':
            temp += '"'
        temp += str(new_values[a])
        if types[a] == 'TEXT':
            temp += '"'
    temp += ' WHERE {}={}'.format(where_field, value)
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(temp)
    conn.commit()
    conn.close()

# Backs up entire database
def backup(db):
    conn = sqlite3.connect(db)
    b_conn = sqlite3.connect('backup1.db')
    conn.backup(b_conn)
    b_conn.close()
    conn.close()