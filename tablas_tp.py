# Importamos bibliotecas
import pandas as pd
import duckdb
from inline_sql import sql, sql_val

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "C:\\Users\\milen\\OneDrive\\Documents\\Trabajos 2024\\Facultad\\Laboratorio de Datos\\enunciado_tablas\\"

equipos_crudo       = pd.read_csv(carpeta+"enunciado_equipos.csv")

jugadores_crudo       = pd.read_csv(carpeta+"enunciado_jugadores.csv")

jugadores_atributos_crudo       = pd.read_csv(carpeta+"enunciado_jugadores_atributos.csv")

partidos_crudo       = pd.read_csv(carpeta+"enunciado_partidos.csv")

liga_crudo       = pd.read_csv(carpeta+"enunciado_liga.csv")

paises_crudo       = pd.read_csv(carpeta+"enunciado_paises.csv")




#%%===========================================================================
#Generamos tabla jugador

consultaSQL = """
                SELECT DISTINCT player_api_id, player_name, birthday
                FROM jugadores_crudo
                ORDER BY player_api_id
            """
jugador = duckdb.sql(consultaSQL).df()

#Uso DISTINCT por si al cargar los datos, subieron datos repetidos

#%%===========================================================================
#Generamos tabla atributo_jugador

consultaSQL = """
                SELECT DISTINCT player_api_id, date, potential
                FROM jugadores_atributos_crudo
                WHERE potential >= 0
            """
atributo_jugador = duckdb.sql(consultaSQL).df()

#Uso DISTINCT por si al cargar los datos, subieron datos repetidos

# Observamos que hay fechas de medición en las que algunos jugadores no se les midió la potencia.
# Esas tuplas no nos aportan información así que las quito de la tabla.
# Los quitamos pidiendo potential >= 0 (potential es un atributo positivo siempre)

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
                SELECT *
                FROM partido_home_02
                
                UNION ALL
                
                SELECT *
                FROM partido_away_02
                
                """
plantel_repes = duckdb.sql(consultaSQL5).df()

consultaSQL = """
                SELECT DISTINCT *
                FROM plantel_repes
                ORDER BY player_api_id
                """

plantel = duckdb.sql(consultaSQL).df()

# En nuestro modelo, cada plantel se mantiene por una temporada entera.
# Ahora a aquellos jugadores que se cambiaron de equipo en medio de una temporada, les asignamos uno de esos dos equipos para la temporada entera (así pertenecen a un solo plantel) con ANYVALUE(team_api_id)

# Quitamos las tuplas con nans provinientes de datos de partidos en los que no se cargaron los jugadores participantes pues no me aportan información del plantel
# Quitamos estas tuplas pidiendo player_api_id >= 0

consultaSQL = """
                SELECT ANY_VALUE(team_api_id) AS team_api_id, season, player_api_id
                FROM plantel
                WHERE player_api_id >= 0
                GROUP BY player_api_id, season
                ORDER BY player_api_id
                """
plantel = duckdb.sql(consultaSQL).df()

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

#%%===========================================================================
#Generamos tabla partido
#Armamos la tabla con el orden de atributos que nos pareció más fácil de seguir para el lector
consultaSQL = """
                SELECT DISTINCT match_api_id, country_id, season, date, home_team_api_id, away_team_api_id, home_team_goal, away_team_goal
                FROM partidos_crudo
            """
partido = duckdb.sql(consultaSQL).df()

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

#Uso DISTINCT por si al cargar los datos, subieron datos repetidos

#%%===========================================================================
#Generamos los csv con nuestras tablas

# Generamos jugador.csv
jugador.to_csv('jugador.csv', index=False)

# Generamos atributo_jugador.csv
atributo_jugador.to_csv('atributo_jugador.csv', index=False)

# Generamos plantel.csv
plantel.to_csv('plantel.csv', index=False)

# Generamos equipo.csv
equipo.to_csv('equipo.csv', index=False)

# Generamos partido.csv
partido.to_csv('partido.csv', index=False)

# Generamos liga_pais.csv
liga_pais.to_csv('liga_pais.csv', index=False)

#%%=======================================
