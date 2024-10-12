# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 16:27:13 2024

@author: Pili
"""
#%%===========================================================================
import pandas as pd
import duckdb
from inline_sql import sql, sql_val

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "C:/Users/Home/Desktop/Info académica Pili/LCD - Pili/2024/2° cuatri/laboratorio de datos/TP Labo/"

equipo = pd.read_csv(carpeta+"equipo.csv")

jugador = pd.read_csv(carpeta+"jugador.csv")

atributo_jugador = pd.read_csv(carpeta+"atributo_jugador.csv")

liga_pais = pd.read_csv(carpeta+"liga_pais.csv")

partido = pd.read_csv(carpeta+"partido.csv")
# %%

consultaSQL = """
                SELECT team_api_id
                FROM equipo
                WHERE country_id = 7809;
            """
prueba_01 = duckdb.sql(consultaSQL).df()
# %%

consultaSQL = """
                SELECT *
                FROM partido
                WHERE country_id = 7809;
            """
partidos_alemania = duckdb.sql(consultaSQL).df()
# %%

consultaSQL = """
                SELECT *
                FROM equipo
                WHERE country_id = 7809;
            """
equipos_alemania = duckdb.sql(consultaSQL).df()
# %%

consultaSQL = """
                SELECT season, COUNT(*) as partidos_por_temporada
                FROM partidos_alemania
                GROUP BY season
            """
partidos_por_temporada = duckdb.sql(consultaSQL).df()
# %%

consultaSQL = """
                SELECT season, home_team_api_id, COUNT(*) as total
                FROM partidos_alemania
                GROUP BY season, home_team_api_id;
            """
cantidad_partidos_por_equipo_y_temporada = duckdb.sql(consultaSQL).df()
# %%

consultaSQL = """
                SELECT home_team_api_id, away_team_api_id, date
                FROM partidos_alemania
                WHERE home_team_goal = away_team_goal AND date LIKE '2013%';
            """
partidos_empatados_2013 = duckdb.sql(consultaSQL).df()

consultaSQL2 = """
                SELECT home_team_api_id, COUNT(*) as total_empates_local
                FROM partidos_empatados_2013
                GROUP BY home_team_api_id;
            """
cantidad_empates_local = duckdb.sql(consultaSQL2).df()

consultaSQL3 = """
                SELECT away_team_api_id, COUNT(*) as total_empates_visitante
                FROM partidos_empatados_2013
                GROUP BY away_team_api_id;
            """
cantidad_empates_visitante = duckdb.sql(consultaSQL3).df()

consultaSQL4 = """
                SELECT DISTINCT (*)
                FROM cantidad_empates_local
                INNER JOIN cantidad_empates_visitante
                ON home_team_api_id = away_team_api_id;
            """
ensamble = duckdb.sql(consultaSQL4).df()

consultaSQL5 = """
                SELECT home_team_api_id AS team_id, 
                total_empates_local + total_empates_visitante AS total_empates
                FROM ensamble;
                """
empates_por_equipo = duckdb.sql(consultaSQL5).df()

consultaSQL6 = """
                SELECT *
                FROM empates_por_equipo
                WHERE total_empates = 17
                """
id_equipo = duckdb.sql(consultaSQL6).df()

consultaSQL7 = """
                SELECT DISTINCT team_long_name, total_empates AS empates_2013
                FROM id_equipo
                INNER JOIN equipo
                ON team_id = team_api_id
            """
consulta_3 = duckdb.sql(consultaSQL7).df()
# %%

consultaSQL = """
                SELECT home_team_api_id, home_team_goal
                FROM partidos_alemania;
            """
goles_local = duckdb.sql(consultaSQL).df()

consultaSQL2 = """
                SELECT match_api_id, away_team_api_id, away_team_goal
                FROM partidos_alemania;
            """
goles_visitante = duckdb.sql(consultaSQL2).df()

consultaSQL3 = """
                SELECT home_team_api_id, SUM(home_team_goal) AS total_goles_local
                FROM goles_local
                GROUP BY home_team_api_id;
            """
suma_goles_local = duckdb.sql(consultaSQL3).df()

consultaSQL4 = """
                SELECT away_team_api_id, SUM(away_team_goal) AS total_goles_visitante
                FROM goles_visitante
                GROUP BY away_team_api_id;
            """
suma_goles_visitante = duckdb.sql(consultaSQL4).df()

consultaSQL5 = """
                SELECT DISTINCT *
                FROM suma_goles_local
                INNER JOIN suma_goles_visitante
                ON home_team_api_id = away_team_api_id;
            """
goles_por_equipo = duckdb.sql(consultaSQL5).df()

consultaSQL6 = """
                SELECT home_team_api_id AS team_id, total_goles_local + total_goles_visitante AS total_goles
                FROM goles_por_equipo
            """
suma_goles_por_equipo = duckdb.sql(consultaSQL6).df()

consultaSQL7 = """
                SELECT *
                FROM suma_goles_por_equipo
                WHERE total_goles = (SELECT MAX(total_goles) FROM suma_goles_por_equipo);
            """
id_equipo2 = duckdb.sql(consultaSQL7).df()

consultaSQL8 = """
                SELECT DISTINCT team_long_name, total_goles
                FROM id_equipo2
                INNER JOIN equipo
                ON team_id = team_api_id
            """
consulta_4 = duckdb.sql(consultaSQL8).df()
