# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 16:27:13 2024

@author: Pili
"""

#%%===========================================================================
import pandas as pd
import duckdb
from inline_sql import sql, sql_val
import numpy as np
import matplotlib.pyplot as plt # Para graficar series multiples
from   matplotlib import ticker   # Para agregar separador de miles
import seaborn as sns           # Para graficar histograma

#%%===========================================================================
# Importamos los datasets que vamos a utilizar en este programa
#=============================================================================

carpeta = "C:\\Users\\milen\\OneDrive\\Documents\\Trabajos 2024\\Facultad\\Laboratorio De Datos\\tablas\\"

equipo = pd.read_csv(carpeta+"equipo.csv")

jugador = pd.read_csv(carpeta+"jugador.csv")

atributo_jugador = pd.read_csv(carpeta+"atributo_jugador.csv")

liga_pais = pd.read_csv(carpeta+"liga_pais.csv")

partido = pd.read_csv(carpeta+"partido.csv")

plantel = pd.read_csv(carpeta + "plantel.csv")

#%%===========================================================================

#EJERCICIO 5

# Nombre del país: Alemania (Germany)
# ID del país: 7809
# Años seleccionados: 2013 a 2016

# %%
# Consultas realizadas para ver qué años era mejor seleccionar

consultaSQL = """
                SELECT *
                FROM partido
                WHERE country_id = 7809
            """

partidos_alemania = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT season, home_team_api_id, COUNT(*) as total
                FROM partidos_alemania
                GROUP BY season, home_team_api_id;
            """
cantidad_partidos_por_equipo_y_temporada = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT season, home_team_api_id as team_id
                FROM cantidad_partidos_por_equipo_y_temporada
                WHERE season = '2008/2009'  
                ORDER BY home_team_api_id ASC;
            """
equipos_en_t1 = duckdb.sql(consultaSQL).df()

consultaSQL2 = """
                SELECT season, home_team_api_id as team_id
                FROM cantidad_partidos_por_equipo_y_temporada
                WHERE season = '2009/2010'
                ORDER BY home_team_api_id ASC;
            """
equipos_en_t2 = duckdb.sql(consultaSQL2).df()

consultaSQL3 = """
                SELECT season, home_team_api_id as team_id
                FROM cantidad_partidos_por_equipo_y_temporada
                WHERE season = '2010/2011'
                ORDER BY home_team_api_id ASC;
            """
equipos_en_t3 = duckdb.sql(consultaSQL3).df()

consultaSQL4= """
                SELECT season, home_team_api_id as team_id
                FROM cantidad_partidos_por_equipo_y_temporada
                WHERE season = '2011/2012'
                ORDER BY home_team_api_id ASC;
            """
equipos_en_t4 = duckdb.sql(consultaSQL4).df()

consultaSQL5= """
                SELECT season, home_team_api_id as team_id
                FROM cantidad_partidos_por_equipo_y_temporada
                WHERE season = '2012/2013'
                ORDER BY home_team_api_id ASC;
            """
equipos_en_t5 = duckdb.sql(consultaSQL5).df()

consultaSQL6= """
                SELECT season, home_team_api_id as team_id
                FROM cantidad_partidos_por_equipo_y_temporada
                WHERE season = '2013/2014'
                ORDER BY home_team_api_id ASC;
            """
equipos_en_t6 = duckdb.sql(consultaSQL6).df()

consultaSQL7= """
                SELECT season, home_team_api_id as team_id
                FROM cantidad_partidos_por_equipo_y_temporada
                WHERE season = '2014/2015'
                ORDER BY home_team_api_id ASC;
            """
equipos_en_t7 = duckdb.sql(consultaSQL7).df()

consultaSQL8= """
                SELECT season, home_team_api_id as team_id
                FROM cantidad_partidos_por_equipo_y_temporada
                WHERE season = '2015/2016'
                ORDER BY home_team_api_id ASC;
            """
equipos_en_t8 = duckdb.sql(consultaSQL8).df()

consultaSQLL = """
                SELECT *
                FROM equipos_en_t6
                LEFT OUTER JOIN equipos_en_t7 ON equipos_en_t6.team_id =  equipos_en_t7.team_id
                LEFT OUTER JOIN equipos_en_t8 ON equipos_en_t7.team_id =  equipos_en_t8.team_id
              """
tabla_prueba = duckdb.sql(consultaSQLL).df()

# Tambien elegimos estos años porque hay especialmente pocos jugadores (solo un 1.4%) que se cambian de equipo en una misma temporada segun los datos crudos


#%%##########################################################################
#                                                                           #
#                               CONSULTAS                                   #
#                                                                           #
#############################################################################
#%%===========================================================================

#¿Cuál es el equipo con mayor cantidad de partidos ganados?

# Convertimos la fecha del partido al tipo "datetime" para poder acceder al año facilmente
partido['date'] = pd.to_datetime(partido['date'], errors='coerce') 

consultaSQL = """
                SELECT *
                FROM partido
                WHERE country_id = 7809 AND home_team_goal != away_team_goal AND 2013 <= YEAR(date) AND YEAR(date) <= 2016
            """

partidos_alemania = duckdb.sql(consultaSQL).df()

# Consideramos solo los partidos ganados (no aquellos con empate de goles)

consultaSQL = """
                SELECT 
                CASE WHEN home_team_goal > away_team_goal
                THEN home_team_api_id
                ELSE away_team_api_id
                END AS winner_team_api_id,
                FROM partidos_alemania
            """
ganadores_alemania = duckdb.sql(consultaSQL).df()

ganadores_alemania['winner_team_api_id'].value_counts().idxmax() # con pandas

consultaSQL = """
                SELECT winner_team_api_id, COUNT(winner_team_api_id) AS cant_partidos_ganados
                FROM ganadores_alemania
                GROUP BY winner_team_api_id
            """
partidos_ganados = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT winner_team_api_id
                FROM partidos_ganados
                WHERE cant_partidos_ganados = (
                    SELECT MAX(cant_partidos_ganados)
                    FROM partidos_ganados)
            """
ganador_maximo_id = duckdb.sql(consultaSQL).df()  # con SQL


consultaSQL = """
                SELECT team_long_name
                FROM equipo, ganador_maximo_id
                WHERE equipo.team_api_id = ganador_maximo_id.winner_team_api_id
            """
            
#Respuesta:            
ganador_maximo_nombre =  duckdb.sql(consultaSQL).df()

#%%===========================================================================
#¿Cuál es el equipo con mayor cantidad de partidos perdidos de cada año?
# Consideramos solo los partidos perdidos (no aquellos con empate de goles)


consultaSQL = """
                SELECT YEAR(date) AS year, 
                CASE WHEN home_team_goal < away_team_goal
                THEN home_team_api_id
                ELSE away_team_api_id
                END AS loser_team_api_id,
                FROM partidos_alemania
                ORDER BY year, loser_team_api_id
            """
            
perdedores_anio_alemania = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT year, loser_team_api_id, COUNT(loser_team_api_id) AS cant_partidos_perdidos
                FROM perdedores_anio_alemania
                GROUP BY year, loser_team_api_id
                ORDER BY year ASC
            """
partidos_perdidos_anio = duckdb.sql(consultaSQL).df() #Lo ordenamos para visualizarlo mejor

consultaSQL = """
                SELECT year, loser_team_api_id
                FROM partidos_perdidos_anio AS p1
                WHERE cant_partidos_perdidos = (
                    SELECT MAX(cant_partidos_perdidos)
                    FROM partidos_perdidos_anio AS p2
                    WHERE p1.year = p2.year)
            """
perdedor_por_anio_id = duckdb.sql(consultaSQL).df() #Si hay más de un equipo con la misma cantidad de partidos perdidos en el mismo año, devuelve todos los correspondientes.

consultaSQL = """
                SELECT year, team_long_name
                FROM perdedor_por_anio_id
                INNER JOIN equipo
                ON perdedor_por_anio_id.loser_team_api_id = equipo.team_api_id
                ORDER BY year ASC
            """

#Respuesta
perdedor_por_anio_nombre = duckdb.sql(consultaSQL).df()

#%%==========================================================================

#¿Cuál es el equipo con mayor cantidad de partidos empatados en el último año?

consultaSQL = """
                SELECT *
                FROM partido
                WHERE country_id = 7809  AND 2013 <= YEAR(date) AND YEAR(date) <= 2016
            """

partidos_alemania_con_empates = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT home_team_api_id, away_team_api_id, date
                FROM partidos_alemania_con_empates
                WHERE home_team_goal = away_team_goal AND YEAR(date) = 2016;
            """
partidos_empatados_2016 = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT home_team_api_id as team_id, COUNT(*) as total_empates
                FROM partidos_empatados_2016
                GROUP BY team_id
                
                UNION ALL
                
                SELECT away_team_api_id as team_id, COUNT(*) as total_empates
                FROM partidos_empatados_2016
                GROUP BY team_id;
            """
cantidad_empates_equipo = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_id, SUM(total_empates) as total_empates
                FROM cantidad_empates_equipo
                GROUP BY team_id;
                """
empates_por_equipo = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT *
                FROM empates_por_equipo
                WHERE total_empates = (SELECT MAX(total_empates) FROM empates_por_equipo);
                """
id_equipo = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT DISTINCT team_long_name, total_empates AS empates_2016
                FROM id_equipo
                INNER JOIN equipo
                ON team_id = team_api_id
            """
            
# Respuesta
consulta_3 = duckdb.sql(consultaSQL).df()

#%%===========================================================================

#¿Cuál es el equipo con mayor cantidad de goles a favor?

consultaSQL = """
                SELECT home_team_api_id, home_team_goal
                FROM partidos_alemania_con_empates
                WHERE YEAR(date) = 2013 OR YEAR(date) = 2014 OR YEAR(date) = 2015 OR YEAR(date) = 2016;
            """
goles_local = duckdb.sql(consultaSQL).df()

consultaSQL2 = """
                SELECT away_team_api_id, away_team_goal
                FROM partidos_alemania_con_empates
                WHERE YEAR(date) = 2013 OR YEAR(date) = 2014 OR YEAR(date) = 2015 OR YEAR(date) = 2016;
            """
goles_visitante = duckdb.sql(consultaSQL2).df()

consultaSQL3 = """
                SELECT home_team_api_id as team_id, SUM(home_team_goal) AS total_goles
                FROM goles_local
                GROUP BY team_id
                
                UNION ALL
                
                SELECT away_team_api_id as team_id, SUM(away_team_goal) AS total_goles
                FROM goles_visitante
                GROUP By team_id
                ORDER BY team_id ASC;
            """
suma_goles = duckdb.sql(consultaSQL3).df()

consultaSQL6 = """
                SELECT team_id, SUM(total_goles) as total_goles
                FROM suma_goles
                GROUP BY team_id
                ORDER BY team_id ASC;
            """
suma_goles_por_equipo = duckdb.sql(consultaSQL6).df()

consultaSQL7 = """
                SELECT *
                FROM suma_goles_por_equipo
                WHERE total_goles = (SELECT MAX(total_goles) FROM suma_goles_por_equipo)
            """
id_equipo2 = duckdb.sql(consultaSQL7).df()

consultaSQL8 = """
                SELECT DISTINCT team_long_name, total_goles
                FROM id_equipo2
                INNER JOIN equipo
                ON team_id = team_api_id
            """
consulta_4 = duckdb.sql(consultaSQL8).df()

#%%===========================================================================

#¿Cuál es el equipo con mayor diferencia de goles? 
# Usamos partidos_alemania que no contiene partidos empatados pues no cambian la diferencia de goles

consultaSQL = """
                SELECT home_team_api_id AS team_api_id, 
                (home_team_goal - away_team_goal) AS team_goal_difference
                FROM partidos_alemania
            """
diferencia_goles_locales = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT away_team_api_id AS team_api_id, 
                (away_team_goal - home_team_goal) AS team_goal_difference
                FROM partidos_alemania
            """
diferencia_goles_visitantes = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT * 
                FROM diferencia_goles_locales
                
                UNION ALL
                
                SELECT * 
                FROM diferencia_goles_visitantes
            """
diferencia_goles_total = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_api_id, SUM(team_goal_difference) AS team_goal_difference
                FROM diferencia_goles_total
                GROUP BY team_api_id
                ORDER BY team_goal_difference DESC
            """
diferencia_goles_por_equipo_id = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_api_id
                FROM diferencia_goles_por_equipo_id
                WHERE team_goal_difference = (
                    SELECT MAX(team_goal_difference)
                    FROM diferencia_goles_por_equipo_id)
                """
id_equipo_con_mayor_diferencia_de_goles = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_long_name
                FROM equipo, id_equipo_con_mayor_diferencia_de_goles
                WHERE equipo.team_api_id = id_equipo_con_mayor_diferencia_de_goles.team_api_id
            """
            
#Respuesta:            
equipo_mayor_diferencia_goles_nombre =  duckdb.sql(consultaSQL).df()

#%%===========================================================================
#¿Cuántos jugadores tuvo durante el periodo de tiempo seleccionado cada equipo en su plantel?

# En nuestro modelo, un plantel se mantiene para cada equipo por una temporada entera.
# Los jugadores que se cambian de equipo en una misma temporada conforman solo el 1.4% de los datos en los años seleccionados.
# Para este ejercicio consideramos solo las temporadas totalmente incluidas en el periodo de tiempo seleccionado

consultaSQL = """
                SELECT DISTINCT *
                FROM plantel
                WHERE (season = '2013/2014' OR season = '2014/2015' OR season = '2015/2016') AND (
                    SELECT country_id
                    FROM equipo
                    WHERE equipo.team_api_id = plantel.team_api_id)
                = 7809
                ORDER BY season, player_api_id
            """
planteles_alemania = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT DISTINCT team_api_id, player_api_id
                FROM planteles_alemania
                ORDER BY team_api_id
            """
planteles_alemania_sin_repes = duckdb.sql(consultaSQL).df()  #ordeno para visualizar mejor

consultaSQL = """
                SELECT team_api_id, COUNT(player_api_id) AS cant_jugadores
                FROM planteles_alemania_sin_repes
                GROUP BY team_api_id
            """
cant_jugadores_por_equipo_id = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_long_name, cant_jugadores
                FROM  cant_jugadores_por_equipo_id
                INNER JOIN equipo
                ON equipo.team_api_id = cant_jugadores_por_equipo_id.team_api_id
            """

#Respuesta
cant_jugadores_por_equipo_nombres = duckdb.sql(consultaSQL).df()

#%%===========================================================================
#¿Cuál es el jugador con mayor cantidad de goles?

#Se habló en clase que no era obligatorio hacer esta consulta

#%%===========================================================================
#¿Cuáles son los jugadores cuyo equipo ganó más partidos?
# De nuevo, no importan los partidos empatados


consultaSQL = """
                SELECT home_team_api_id AS team_api_id, season
                FROM partidos_alemania
                WHERE home_team_goal > away_team_goal
                
                 UNION ALL   
 
                SELECT away_team_api_id AS team_api_id, season
                FROM partidos_alemania
                WHERE away_team_goal > home_team_goal
                
                ORDER BY team_api_id, season 
            """
partidos_ganados_temporada= duckdb.sql(consultaSQL).df()


consultaSQL = """
                SELECT team_api_id, season, COUNT(*) AS wins
                FROM partidos_ganados_temporada
                GROUP BY team_api_id, season
                ORDER BY team_api_id, season
            """
cant_partidos_ganados_por_equipo_temporada= duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT cant_partidos_ganados_por_equipo_temporada.team_api_id, cant_partidos_ganados_por_equipo_temporada.season, wins, player_api_id
                FROM cant_partidos_ganados_por_equipo_temporada
                INNER JOIN planteles_alemania
                ON cant_partidos_ganados_por_equipo_temporada.team_api_id = planteles_alemania.team_api_id AND cant_partidos_ganados_por_equipo_temporada.season = planteles_alemania.season
            """
cant_partidos_ganados_por_jugador_temporada= duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_api_id, SUM(wins) AS wins
                FROM cant_partidos_ganados_por_jugador_temporada
                GROUP BY player_api_id
                ORDER BY wins
            """

cant_partidos_ganados_por_jugador_id= duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_api_id
                FROM cant_partidos_ganados_por_jugador_id
                WHERE wins = (
                    SELECT MAX(wins)
                    FROM cant_partidos_ganados_por_jugador_id)
                """
jugadores_equipo_ganador_ids= duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_name
                FROM jugadores_equipo_ganador_ids, jugador
                WHERE jugadores_equipo_ganador_ids.player_api_id = jugador.player_api_id
            """
            
# Respuesta            
jugadores_equipo_ganador_nombres = duckdb.sql(consultaSQL).df()

#%%===========================================================================

#¿Cuál es el jugador que estuvo en más equipos?

consultaSQL = """
                SELECT player_api_id, COUNT(team_api_id) AS cant_equipos_en_los_que_estuvo
                FROM planteles_alemania_sin_repes
                GROUP BY player_api_id
                ORDER BY cant_equipos_en_los_que_estuvo DESC
            """
cant_equipos_en_los_que_jugo_cada_jugador = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_api_id
                FROM cant_equipos_en_los_que_jugo_cada_jugador
                WHERE  cant_equipos_en_los_que_estuvo = (
                    SELECT MAX(cant_equipos_en_los_que_estuvo)
                    FROM cant_equipos_en_los_que_jugo_cada_jugador)
                """
id_jugador_en_mas_equipos = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_name
                FROM id_jugador_en_mas_equipos, jugador
                WHERE id_jugador_en_mas_equipos.player_api_id = jugador.player_api_id
            """
            
#Respuesta:            
nombre_jugador_en_mas_equipos =  duckdb.sql(consultaSQL).df()


#%%===========================================================================

#¿Cuál es el jugador que menor variación de potencia ha tenido a lo largo de los años?
# Tomamos la variación de potencia como la diferencia entre el máximo y mínimo de potencia para cada jugador medido en los años 2013 a 2016


# Convertimos la fecha del partido al tipo "datetime" para poder acceder al año facilmente
atributo_jugador['date'] = pd.to_datetime(atributo_jugador['date'], errors='coerce') 

consultaSQL = """
                SELECT DISTINCT player_api_id
                FROM plantel
                WHERE (
                    SELECT country_id
                    FROM equipo
                    WHERE equipo.team_api_id = plantel.team_api_id)
                = 7809
                ORDER BY player_api_id
            """
jugadores_alemania = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_api_id, date, potential
                FROM atributo_jugador
                WHERE YEAR(date) = 2013 OR YEAR(date) = 2014 OR YEAR(date) = 2015 OR YEAR(date) = 2016
            """
atributo_jugador_anio = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT atributo_jugador_anio.player_api_id, date, potential
                FROM atributo_jugador_anio
                INNER JOIN jugadores_alemania
                ON atributo_jugador_anio.player_api_id = jugadores_alemania.player_api_id
                ORDER BY atributo_jugador_anio.player_api_id, date
            """
jugadores_alemania_potencias = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_api_id, (MAX(potential) - MIN(potential)) AS variation_potential
                FROM jugadores_alemania_potencias
                GROUP BY player_api_id
                ORDER BY variation_potential
            """
jugadores_alemania_potencias_var = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_api_id
                FROM jugadores_alemania_potencias_var
                WHERE variation_potential = (
                    SELECT MIN(variation_potential)
                    FROM jugadores_alemania_potencias_var)
            """
jugadores_menor_variacion = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_name
                FROM jugadores_menor_variacion, jugador
                WHERE jugadores_menor_variacion.player_api_id = jugador.player_api_id
            """
#Respuesta:
    
jugadores_menor_variacion = duckdb.sql(consultaSQL).df()


#%%##########################################################################
#                                                                           #
#                             VISUALIZACIONES                               #
#                                                                           #
#############################################################################
#%%===========================================================================

#Graficar la cantidad de goles a favor y en contra de cada equipo a lo largo de los años que elijan

#%%

# GOLES A FAVOR POR EQUIPO

partido['date'] = pd.to_datetime(partido['date'], errors='coerce') 

consultaSQL = """
                SELECT *
                FROM partido
                WHERE country_id = 7809  AND 2013 <= YEAR(date) AND YEAR(date) <= 2016
            """

partidos_alemania_con_empates = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT home_team_api_id, home_team_goal, YEAR(date) AS year
                FROM partidos_alemania_con_empates
                WHERE YEAR(date) = 2013 OR YEAR(date) = 2014 OR YEAR(date) = 2015 OR YEAR(date) = 2016;
            """
goles_local = duckdb.sql(consultaSQL).df()

consultaSQL2 = """
                SELECT away_team_api_id, away_team_goal, YEAR(date) AS year
                FROM partidos_alemania_con_empates
                WHERE YEAR(date) = 2013 OR YEAR(date) = 2014 OR YEAR(date) = 2015 OR YEAR(date) = 2016;
            """
goles_visitante = duckdb.sql(consultaSQL2).df()

consultaSQL3 = """
                SELECT year, home_team_api_id as team_id, SUM(home_team_goal) AS total_goles
                FROM goles_local
                GROUP BY team_id, year
                
                UNION ALL
                
                SELECT year, away_team_api_id as team_id, SUM(away_team_goal) AS total_goles
                FROM goles_visitante
                GROUP By team_id, year
                ORDER BY team_id ASC;
            """
suma_goles = duckdb.sql(consultaSQL3).df()

consultaSQL6 = """
                SELECT year, team_id, SUM(total_goles) as total_goles
                FROM suma_goles
                GROUP BY team_id, year
                ORDER BY team_id ASC;
            """
suma_goles_por_equipo = duckdb.sql(consultaSQL6).df()

consultaSQL8 = """
                SELECT DISTINCT year, team_long_name, total_goles
                FROM suma_goles_por_equipo
                INNER JOIN equipo
                ON team_id = team_api_id
                ORDER BY year 
            """
goles_a_favor_por_equipo = duckdb.sql(consultaSQL8).df()


#%%

#### GRÁFICO: HEATMAP 


df = goles_a_favor_por_equipo.pivot(index='team_long_name', columns='year', values='total_goles') # Cargamos los datos

# Generamos el grafico mejorando la información mostrada
g = sns.clustermap(df,
               row_cluster = False,     # elimina el dendograma izquierdo
               col_cluster = False,    # elimina el dendograma superior
               cmap = "Greens",         # paleta de colores
               standard_scale = 1)     # estandarizamos los datos de c/ columna

# Añadir título
plt.title("Goles a Favor por Equipo a lo Largo de los Años", size=16)

# Cambiar la etiqueta del eje Y
g.ax_heatmap.set_ylabel("Nombre del Equipo", size=12)

plt.show()

#%%
# GOLES EN CONTRA POR EQUIPO

consultaSQL = """
                SELECT home_team_api_id, away_team_goal, YEAR(date) AS year
                FROM partidos_alemania_con_empates
                WHERE YEAR(date) = 2013 OR YEAR(date) = 2014 OR YEAR(date) = 2015 OR YEAR(date) = 2016;
            """
goles_en_contra_local = duckdb.sql(consultaSQL).df()

consultaSQL2 = """
                SELECT away_team_api_id, home_team_goal, YEAR(date) AS year
                FROM partidos_alemania_con_empates
                WHERE YEAR(date) = 2013 OR YEAR(date) = 2014 OR YEAR(date) = 2015 OR YEAR(date) = 2016;
            """
goles_en_contra_visitante = duckdb.sql(consultaSQL2).df()

consultaSQL3 = """
                SELECT year, home_team_api_id as team_id, SUM(away_team_goal) AS total_goles
                FROM goles_en_contra_local
                GROUP BY team_id, year
                
                UNION ALL
                
                SELECT year, away_team_api_id as team_id, SUM(home_team_goal) AS total_goles
                FROM goles_en_contra_visitante
                GROUP By team_id, year
                ORDER BY team_id ASC;
            """
suma_goles_en_contra = duckdb.sql(consultaSQL3).df()

consultaSQL6 = """
                SELECT year, team_id, SUM(total_goles) as total_goles
                FROM suma_goles_en_contra
                GROUP BY team_id, year
                ORDER BY team_id ASC;
            """
suma_goles_en_contra_por_equipo = duckdb.sql(consultaSQL6).df()

consultaSQL8 = """
                SELECT DISTINCT year, team_long_name, total_goles
                FROM suma_goles_en_contra_por_equipo
                INNER JOIN equipo
                ON team_id = team_api_id
                ORDER BY year 
            """
goles_en_contra_por_equipo = duckdb.sql(consultaSQL8).df()

#%%

#### GRÁFICO: HEATMAP

df = goles_en_contra_por_equipo.pivot(index='team_long_name', columns='year', values='total_goles') # Cargamos los datos

# Generamos el grafico mejorando la información mostrada
g = sns.clustermap(df,
               row_cluster = False,    # elimina el dendograma izquierdo
               col_cluster = False,    # elimina el dendograma superior
               cmap = "Reds",         # paleta de colores
               standard_scale = 1)     # estandarizamos los datos de c/ columna
# Añadir título
plt.title("Goles en Contra por Equipo a lo Largo de los Años", size=16)

# Cambiar la etiqueta del eje Y
g.ax_heatmap.set_ylabel("Nombre del Equipo", size=12)

plt.show()

#%%==============================================================================================================================
# Graficamos el promedio de gol de los equipos a lo largo de los años 2013 a 2016
# Solo graficaremos aquellos equipos que hayan jugado al menos una vez en cada año


consultaSQL = """
                SELECT home_team_api_id AS team_api_id, date
                FROM partidos_alemania_con_empates
                
                UNION ALL
                
                SELECT away_team_api_id AS team_api_id, date
                FROM partidos_alemania_con_empates
            """
ids_equipos = duckdb.sql(consultaSQL).df()

consultaSQL = """
        SELECT DISTINCT team_api_id, YEAR(date) AS year
        FROM  ids_equipos
        WHERE YEAR(date) = 2013 
        ORDER BY team_api_id
            """
ids_equipos_2013 = duckdb.sql(consultaSQL).df()

consultaSQL = """
        SELECT DISTINCT team_api_id, YEAR(date) AS year
        FROM  ids_equipos
        WHERE YEAR(date) = 2014 
        ORDER BY team_api_id
            """
ids_equipos_2014 = duckdb.sql(consultaSQL).df()

consultaSQL = """
        SELECT DISTINCT team_api_id, YEAR(date) AS year
        FROM  ids_equipos
        WHERE YEAR(date) = 2015 
        ORDER BY team_api_id
            """
ids_equipos_2015 = duckdb.sql(consultaSQL).df()

consultaSQL = """
        SELECT DISTINCT team_api_id, YEAR(date) AS year
        FROM  ids_equipos
        WHERE YEAR(date) = 2016 
        ORDER BY team_api_id
            """
ids_equipos_2016 = duckdb.sql(consultaSQL).df()

consultaSQL = """
        SELECT team_api_id
        FROM (SELECT *
              FROM ids_equipos_2013
              
              UNION ALL 
              
              SELECT *
              FROM ids_equipos_2014
              
              UNION ALL
              
              SELECT *
              FROM ids_equipos_2015
              
              UNION ALL
              
              SELECT *
              FROM ids_equipos_2016
              )
        ORDER BY team_api_id
            """
ids_equipos = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_api_id, COUNT(team_api_id) AS anios_jugados
                FROM ids_equipos
                GROUP BY team_api_id
                ORDER BY team_api_id
            """
ids_equipos = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_api_id
                FROM ids_equipos
                WHERE anios_jugados = 4
                ORDER BY team_api_id
            """
ids_equipos = duckdb.sql(consultaSQL).df() #esta es la lista de ids de equipos que jugaron al menos una vez en cada año del periodo seleccionado


consultaSQL = """
                SELECT YEAR(date) AS year, home_team_api_id AS team_api_id, home_team_goal AS goals
                FROM partidos_alemania_con_empates
                INNER JOIN ids_equipos
                ON ids_equipos.team_api_id = partidos_alemania_con_empates.home_team_api_id
                
                UNION ALL
                
                SELECT YEAR(date) AS year, away_team_api_id AS team_api_id, away_team_goal AS goals
                FROM partidos_alemania_con_empates
                INNER JOIN ids_equipos
                ON ids_equipos.team_api_id = partidos_alemania_con_empates.away_team_api_id
                
                ORDER BY team_api_id, year
            """
goles_por_anio = duckdb.sql(consultaSQL).df()


consultaSQL = """
                SELECT DISTINCT year, team_long_name AS team, AVG(goals) AS avg_goals
                FROM goles_por_anio
                INNER JOIN equipo
                ON goles_por_anio.team_api_id = equipo.team_api_id
                GROUP BY team, year
                ORDER BY year
            """
promedio_goles_por_anio = duckdb.sql(consultaSQL).df()


#%% 
### GRÁFICO: GRÁFICO DE LÍNEA

df = promedio_goles_por_anio.pivot(index='year', columns='team', values='avg_goals')

# Generamos el grafico 
fig, ax = plt.subplots()

# Itera sobre todas las columnas (equipos) y las grafica
for team in df.columns:
    ax.plot(df.index, df[team], 
            marker='.', linestyle='-', linewidth=0.5, label=team)

# Agregamos título, etiquetas y leyenda
ax.set_title('Promedio goles por año para los equipos')
ax.set_xlabel('Año')
ax.set_ylabel('Promedio de goles por partido')

# Ajustamos los límites del gráfico
ax.set_xlim(df.index.min()-0.15, df.index.max()+0.15)
ax.set_ylim(0, df.max().max())

# Configura las etiquetas del eje X para mostrar solo 2013, 2014, 2015 y 2016
ax.set_xticks([2013, 2014, 2015, 2016])

# Muestra la leyenda con los nombres de los equipos
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')  # Leyenda fuera del gráfico para que no lo superponga

# Ajusta el gráfico para no cortar la leyenda
plt.tight_layout()

# Muestra el gráfico
plt.show()


#%%===========================================================================
# Graficar la diferencia de goles convertidos jugando de local vs visitante a lo largo del tiempo.
# Entendemos esta consigna como "graficar la diferencia de goles que tuvo cada equipo jugando de local en cada año"  
# y "graficar la diferencia de goles que tuvo cada equipo jugando de visitante en cada año" 


consultaSQL = """
                SELECT home_team_api_id AS team_api_id, YEAR(date) AS year,
                (home_team_goal - away_team_goal) AS team_goal_difference
                FROM partidos_alemania
                INNER JOIN ids_equipos
                ON ids_equipos.team_api_id = partidos_alemania.home_team_api_id
                ORDER BY team_api_id
            """
diferencia_goles_locales_anio = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_api_id, year, SUM(team_goal_difference) AS team_goal_difference
                FROM diferencia_goles_locales_anio
                GROUP BY team_api_id, year
                ORDER BY team_api_id, year
            """
diferencia_goles_locales_total_anio = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT DISTINCT year, team_long_name AS team, team_goal_difference
                FROM diferencia_goles_locales_total_anio
                INNER JOIN equipo
                ON diferencia_goles_locales_total_anio.team_api_id = equipo.team_api_id
                ORDER BY team_goal_difference
            """
diferencia_goles_locales_total_anio = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT away_team_api_id AS team_api_id, YEAR(date) AS year,
                (away_team_goal - home_team_goal) AS team_goal_difference
                FROM partidos_alemania
                INNER JOIN ids_equipos
                ON ids_equipos.team_api_id = partidos_alemania.away_team_api_id
                ORDER BY team_api_id
            """
diferencia_goles_visitantes_anio = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT team_api_id, year, SUM(team_goal_difference) AS team_goal_difference
                FROM diferencia_goles_visitantes_anio
                GROUP BY team_api_id, year
                ORDER BY team_api_id, year
            """
diferencia_goles_visitantes_total_anio = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT DISTINCT year, team_long_name AS team, team_goal_difference
                FROM diferencia_goles_visitantes_total_anio
                INNER JOIN equipo
                ON diferencia_goles_visitantes_total_anio.team_api_id = equipo.team_api_id
                ORDER BY team_goal_difference ASC
            """
diferencia_goles_visitantes_total_anio = duckdb.sql(consultaSQL).df()

#%%

###GRÁFICO: GRÁFICO DE LÍNEA
#Graficamos la diferencia de goles de local

df = diferencia_goles_locales_total_anio.pivot(index='year', columns='team', values='team_goal_difference')

# Generamos el grafico 
fig, ax = plt.subplots()

# Itera sobre todas las columnas (equipos) y las grafica
for team in df.columns:
    ax.plot(df.index, df[team], 
            marker='.', linestyle='-', linewidth=0.5, label=team)

# Agregamos título, etiquetas y leyenda
ax.set_title('Diferencia de goles de local por año')
ax.set_xlabel('Año')
ax.set_ylabel('Total de diferencia de goles de local')

# Ajustamos los límites del gráfico
ax.set_xlim(df.index.min() -0.15, df.index.max()+0.15)
ax.set_ylim(-19, df.max().max()+1)

# Configura las etiquetas del eje X para mostrar solo 2013, 2014, 2015 y 2016
ax.set_xticks([2013, 2014, 2015, 2016])

# Muestra la leyenda con los nombres de los equipos
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')  # Leyenda fuera del gráfico para que no lo superponga

# Ajusta el gráfico para no cortar la leyenda
plt.tight_layout()

# Muestra el gráfico
plt.show()

#%%

###GRÁFICO: GRÁFICO DE LÍNEA
#Graficamos las diferencias de goles hechas de visitante

df = diferencia_goles_visitantes_total_anio.pivot(index='year', columns='team', values='team_goal_difference')

# Generamos el grafico 
fig, ax = plt.subplots()

# Itera sobre todas las columnas (equipos) y las grafica
for team in df.columns:
    ax.plot(df.index, df[team], 
            marker='.', linestyle='-', linewidth=0.5, label=team)

# Agregamos título, etiquetas y leyenda
ax.set_title('Diferencia de goles de visitante por año')
ax.set_xlabel('Año')
ax.set_ylabel('Total de diferencia de goles de visitante')

# Ajustamos los límites del gráfico
ax.set_xlim(df.index.min() -0.15, df.index.max()+0.15)
ax.set_ylim(-25, df.max().max()+1)

# Configura las etiquetas del eje X para mostrar solo 2013, 2014, 2015 y 2016
ax.set_xticks([2013, 2014, 2015, 2016])

# Muestra la leyenda con los nombres de los equipos
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')  # Leyenda fuera del gráfico para que no lo superponga

# Ajusta el gráfico para no cortar la leyenda
plt.tight_layout()

# Muestra el gráfico
plt.show()

#%%===========================================================================
# Graficar el número de goles convertidos por cada equipo en función de la suma de sus atributos
# Vamos a tomar el total de goles convertidos por equipo por temporada para que los atributos refieran al mismo plantel
# Solo consideramos aquellas temporadas completamente incluidas en nuestros años seleccionados
# Elegimos los atributos potencial, velocidad, agilidad y reacción para este gráfico pues nos parecieron los más relevantes y son datos que varían bastante de jugador a jugador

consultaSQL = """
                SELECT home_team_api_id AS team_api_id, home_team_goal AS goals, season
                FROM partidos_alemania_con_empates
                WHERE season = '2013/2014' OR season = '2014/2015' OR season = '2015/2016'
                
                UNION ALL

                SELECT away_team_api_id AS team_api_id, away_team_goal AS goals, season
                FROM partidos_alemania_con_empates
                WHERE season = '2013/2014' OR season = '2014/2015' OR season = '2015/2016'
            """
goles_por_temporada = duckdb.sql(consultaSQL).df()


consultaSQL = """
                SELECT  season, team_api_id, SUM(goals) AS goals
                FROM goles_por_temporada
                GROUP BY team_api_id, season
            """
goles_por_temporada = duckdb.sql(consultaSQL).df()

atributo_jugador['date'] = pd.to_datetime(atributo_jugador['date'], errors='coerce') 

consultaSQL = """
                SELECT jugadores_alemania.player_api_id, date, potential, sprint_speed, agility, reactions
                FROM atributo_jugador
                INNER JOIN jugadores_alemania
                ON jugadores_alemania.player_api_id = atributo_jugador.player_api_id
                WHERE YEAR(date) <= 2016 AND YEAR(date) >= 2013
                ORDER BY jugadores_alemania.player_api_id, date
            """

atributo_jugador_alemania = duckdb.sql(consultaSQL).df()

# Voy a considerar el promedio de la suma del promedio de las mediciones tomadas en 2013 y 2014 para cada jugador como el valor de suma de sus atributos en la temporada 2013/2014 y así con las demás temporadas
# pues nos pareció la forma más justa de representar los atributos de cada jugador durante una temporada

consultaSQL = """
                SELECT player_api_id, AVG(potential) as avg_potential, AVG(sprint_speed) AS avg_sprint_speed, AVG(agility) AS avg_agility, AVG(reactions) AS avg_reactions, YEAR(date) AS year
                FROM atributo_jugador_alemania
                GROUP BY player_api_id, year
                ORDER BY player_api_id, year
            """

atributo_jugador_alemania = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_api_id, year, (avg_potential + avg_sprint_speed + avg_agility + avg_reactions) AS suma_atributos
                FROM atributo_jugador_alemania 
                ORDER BY player_api_id, year
            """

suma_atributo_jugador_alemania = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT player_api_id, AVG(suma_atributos) AS suma_atributos, '2013/2014' AS season
                FROM suma_atributo_jugador_alemania 
                WHERE year = 2013 OR year = 2014
                GROUP BY player_api_id
                
                UNION ALL
                
                SELECT player_api_id, AVG(suma_atributos) AS suma_atributos, '2014/2015' AS season
                FROM suma_atributo_jugador_alemania 
                WHERE year = 2014 OR year = 2015
                GROUP BY player_api_id                
                
                UNION ALL
                
                SELECT player_api_id, AVG(suma_atributos) AS suma_atributos, '2015/2016' AS season
                FROM suma_atributo_jugador_alemania 
                WHERE year = 2015 OR year = 2016
                GROUP BY player_api_id
                
                
            """

suma_atributo_jugador_alemania_temporada = duckdb.sql(consultaSQL).df()

# Vamos a considerar el promedio de las "sumas de atributos" de todos los jugador en el plantel para no favorecer a equipos con más jugadores en su plantel injustamente

consultaSQL = """
                SELECT team_api_id, planteles_alemania.season, AVG(suma_atributos) AS suma_atributos
                FROM suma_atributo_jugador_alemania_temporada
                INNER JOIN planteles_alemania
                ON suma_atributo_jugador_alemania_temporada.player_api_id = planteles_alemania.player_api_id AND suma_atributo_jugador_alemania_temporada.season = planteles_alemania.season
                GROUP BY team_api_id, planteles_alemania.season
                ORDER BY team_api_id, planteles_alemania.season
            """
suma_atributo_equipos_alemania_temporada = duckdb.sql(consultaSQL).df()

consultaSQL = """
                SELECT suma_atributo_equipos_alemania_temporada.season,  suma_atributos, goals
                FROM suma_atributo_equipos_alemania_temporada
                INNER JOIN goles_por_temporada
                ON suma_atributo_equipos_alemania_temporada.team_api_id = goles_por_temporada.team_api_id AND suma_atributo_equipos_alemania_temporada.season = goles_por_temporada.season
                ORDER BY suma_atributo_equipos_alemania_temporada.season
            """
goles_en_funcion_de_atributos_por_temporada = duckdb.sql(consultaSQL).df()

#%%
### GRÁFICO: SCATTER GRAPH

df = goles_en_funcion_de_atributos_por_temporada

# Separar en dos subconjuntos: vinos blancos y tintos

season_2013_2014 = df[df['season'] == '2013/2014']
season_2014_2015 = df[df['season'] == '2014/2015']
season_2015_2016 = df[df['season'] == '2015/2016']


# Crear gráfico de dispersión para ambos
fig, ax = plt.subplots()

ax.scatter(season_2013_2014['suma_atributos'], season_2013_2014['goals'], 
           color='yellow', label='2013/2014', s=8)
ax.scatter(season_2014_2015['suma_atributos'], season_2014_2015['goals'], 
           color='orange', label='2014/2015', s=8)
ax.scatter(season_2015_2016['suma_atributos'], season_2015_2016['goals'], 
           color='red', label='2015/2016', s=8)


# Título y etiquetas
ax.set_title('Goles convertidos por temporada según promedio de suma de atributos del plantel')
ax.set_xlabel('promedio de suma de atributos', fontsize='medium')
ax.set_ylabel('goles convertidos', fontsize='medium')

# Agregar una leyenda para distinguir tipos de vino
ax.legend()

plt.show()

#El gráfico muestra una relación positiva bastante clara. A mayor promedio de suma de atributos del plantel, mayor cantidad de goles convierte el equipo




