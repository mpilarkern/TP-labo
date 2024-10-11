import pandas as pd
import duckdb
from inline_sql import sql, sql_val

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "C:/Users/Home/Desktop/Info académica Pili/LCD - Pili/2024/2° cuatri/laboratorio de datos/TP Labo/CSV/"

equipos_crudo       = pd.read_csv(carpeta+"enunciado_equipos.csv")

jugadores_crudo       = pd.read_csv(carpeta+"enunciado_jugadores.csv")

jugadores_atributos_crudo       = pd.read_csv(carpeta+"enunciado_jugadores_atributos.csv")

liga_crudo       = pd.read_csv(carpeta+"enunciado_liga.csv")

paises_crudo       = pd.read_csv(carpeta+"enunciado_paises.csv")

partidos_crudo = pd.read_csv(carpeta+"enunciado_partidos.csv")

print(jugadores_crudo.columns)
print(jugadores_atributos_crudo.columns)

#%%
#Veamos si cada equipo mantiene un mismo plantel por temporada

consultaSQL = """
                SELECT *
                FROM partidos_crudo
                WHERE season = '2011/2012' AND home_team_api_id= '9994'
            """
prueba_01 = duckdb.sql(consultaSQL).df()

print(prueba_01)
#%%===========================================================================

#convierto el atributo "date" a datetime para poder realizar comparaciones
jugadores_atributos_crudo['date'] = pd.to_datetime(jugadores_atributos_crudo['date'], errors='coerce')

#ordeno los jugadores según su id y la fecha más reciente
jugadores_ordenados = jugadores_atributos_crudo.sort_values(by=['player_api_id', 'date'], ascending=[True, False])

# Nos quedamos con la tupla de fecha más reciente para cada 'player_api_id'
jugadores_recientes = jugadores_ordenados.drop_duplicates(subset='player_api_id', keep='first')

# Generar las tablas que 
consultaSQL = """
                SELECT DISTINCT jugadores_recientes.*, jugadores_crudo.birthday,jugadores_crudo.height,jugadores_crudo.weight,jugadores_crudo.player_name
                FROM jugadores_recientes
                INNER JOIN jugadores_crudo
                ON jugadores_crudo.player_api_id = jugadores_recientes.player_api_id
              """

jugadores_01 = duckdb.sql(consultaSQL).df()
#%%===========================================================================
#Generamos tabla jugador

consultaSQL = """
                SELECT DISTINCT player_api_id, player_name, birthday
                FROM jugadores_crudo
            """
jugador = duckdb.sql(consultaSQL).df()
jugador.to_csv('jugador.csv', index=False)

#Uso DISTINCT por si al cargar los datos, subieron datos repetidos

#%%===========================================================================
#Generamos tabla atributo jugador

consultaSQL = """
                SELECT DISTINCT player_api_id, date, potential
                FROM jugadores_atributos_crudo
            """
atributo_jugador = duckdb.sql(consultaSQL).df()
atributo_jugador.to_csv('atributo_jugador.csv', index=False)

#Uso DISTINCT por si al cargar los datos, subieron datos repetidos

#%%===========================================================================
#Generamos tabla plantel
consultaSQL1 = """
                SELECT DISTINCT home_team_api_id AS team_api_id, season, home_player_1,home_player_2,home_player_3,home_player_4,home_player_5,home_player_6,home_player_7,home_player_8,home_player_9,home_player_10,home_player_11
                FROM partidos_crudo
            """
partido_home = duckdb.sql(consultaSQL1).df()

consultaSQL2 = """
                SELECT team_api_id, season, home_player_1 AS player_api_id
                FROM partido_home

                UNION ALL

                SELECT team_api_id, season, home_player_2 AS player_api_id
                FROM partido_home

                UNION ALL

                SELECT team_api_id, season, home_player_3 AS player_api_id
                FROM partido_home

                UNION ALL

                SELECT team_api_id, season, home_player_4 AS player_api_id
                FROM partido_home
                
                UNION ALL
                
                SELECT team_api_id, season, home_player_5 AS player_api_id
                FROM partido_home

                UNION ALL

                SELECT team_api_id, season, home_player_6 AS player_api_id
                FROM partido_home
                
                UNION ALL
                
                SELECT team_api_id, season, home_player_7 AS player_api_id
                FROM partido_home

                UNION ALL

                SELECT team_api_id, season, home_player_8 AS player_api_id
                FROM partido_home
                
                UNION ALL
                
                SELECT team_api_id, season, home_player_9 AS player_api_id
                FROM partido_home

                UNION ALL

                SELECT team_api_id, season, home_player_10 AS player_api_id
                FROM partido_home
                
                UNION ALL
                
                SELECT team_api_id, season, home_player_11 AS player_api_id
                FROM partido_home
                """
                
partido_home_02 = duckdb.sql(consultaSQL2).df()

consultaSQL3 = """
                SELECT DISTINCT away_team_api_id AS team_api_id, season, away_player_1,away_player_2,away_player_3,away_player_4,away_player_5,away_player_6,away_player_7,away_player_8,away_player_9,away_player_10,away_player_11
                FROM partidos_crudo
            """
partido_away = duckdb.sql(consultaSQL3).df()

consultaSQL4 = """
                SELECT team_api_id, season, away_player_1 AS player_api_id
                FROM partido_away

                UNION ALL

                SELECT team_api_id, season, away_player_2 AS player_api_id
                FROM partido_away

                UNION ALL

                SELECT team_api_id, season, away_player_3 AS player_api_id
                FROM partido_away

                UNION ALL

                SELECT team_api_id, season, away_player_4 AS player_api_id
                FROM partido_away
                
                UNION ALL
                
                SELECT team_api_id, season, away_player_5 AS player_api_id
                FROM partido_away

                UNION ALL

                SELECT team_api_id, season, away_player_6 AS player_api_id
                FROM partido_away
                
                UNION ALL
                
                SELECT team_api_id, season, away_player_7 AS player_api_id
                FROM partido_away

                UNION ALL

                SELECT team_api_id, season, away_player_8 AS player_api_id
                FROM partido_away
                
                UNION ALL
                
                SELECT team_api_id, season, away_player_9 AS player_api_id
                FROM partido_away

                UNION ALL

                SELECT team_api_id, season, away_player_10 AS player_api_id
                FROM partido_away
                
                UNION ALL
                
                SELECT team_api_id, season, away_player_11 AS player_api_id
                FROM partido_away
                """
                
partido_away_02 = duckdb.sql(consultaSQL4).df()

consultaSQL5 = """
                SELECT DISTINCT *
                FROM partido_home_02

                UNION ALL

                SELECT DISTINCT *
                FROM partido_away_02

                """
plantel = duckdb.sql(consultaSQL5).df()
plantel.to_csv('plantel.csv', index=False)


#%%===========================================================================
#Generamos tabla equipo
consultaSQL1 = """
                SELECT DISTINCT team_api_id, team_long_name, team_short_name
                FROM equipos_crudo
            """
equipo_sin_pais = duckdb.sql(consultaSQL1).df()

consultaSQL2 = """
                SELECT DISTINCT home_team_api_id, country_id
                FROM partidos_crudo
            """
equipo_pais_home = duckdb.sql(consultaSQL2).df()

#Ya verificamos y todos los equipos juegan de local algún partido en el dataset así que no excluyo a ninguno

consultaSQL3 = """
                SELECT DISTINCT team_api_id, team_long_name, team_short_name, country_id
                FROM equipo_sin_pais
                INNER JOIN equipo_pais_home
                ON team_api_id = home_team_api_id
            """
equipo = duckdb.sql(consultaSQL3).df()
equipo.to_csv('equipo.csv', index=False)


#%%===========================================================================
#Generamos tabla partido
#Armamos la tabla con el orden de atributos que nos pareció más fácil de seguir para el lector
consultaSQL = """
                SELECT DISTINCT match_api_id, country_id, season, date, home_team_api_id, away_team_api_id, home_team_goal, away_team_goal
                FROM partidos_crudo
            """
partido = duckdb.sql(consultaSQL).df()
partido.to_csv('partido.csv', index=False)
#Mantenemos el country_id a pesar de poder acceder a él a través de home_team_api_id y away_team_api_id pues es una consulta frecuente y ahorra tener que hacer joins repetidamente luego

#%%===========================================================================
#Generamos tabla liga_pais

consultaSQL = """
                SELECT DISTINCT pais.id AS country_id, pais.country_code, pais.name AS country_name, liga.name AS league_name
                FROM paises_crudo AS 'pais'
                INNER JOIN liga_crudo AS 'liga'
                ON pais.id = liga.country_id
            """
liga_pais = duckdb.sql(consultaSQL).df()
liga_pais.to_csv('liga_pais.csv', index=False)
