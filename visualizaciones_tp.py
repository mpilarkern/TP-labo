# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 22:47:15 2024

@author: milen
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

#%%===========================================================================
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

#### 9. HEATMAP + DENDOGRAMA


df = goles_a_favor_por_equipo.pivot(index='team_long_name', columns='year', values='total_goles') # Cargamos los datos


# Rellenamos los valores NaN con 0
df = df.fillna(0)

# Generamos el grafico por defecto
#sns.clustermap(df) 
# Las escalas de las distintas columnas son muy distintas!
# Necesitamos estandarizar los datos a la hora de graficar

# Generamos el grafico mejorando la información mostrada
sns.clustermap(df,
               col_cluster = False,    # elimina el dendograma superior
               cmap = "Greens",         # paleta de colores
               standard_scale = 1)     # estandarizamos los datos de c/ columna
plt.show()

#%%===========================================================================

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

#### 9. HEATMAP + DENDOGRAMA


df = goles_en_contra_por_equipo.pivot(index='team_long_name', columns='year', values='total_goles') # Cargamos los datos


# Rellenamos los valores NaN con 0
df = df.fillna(0)

# Generamos el grafico por defecto
#sns.clustermap(df) 
# Las escalas de las distintas columnas son muy distintas!
# Necesitamos estandarizar los datos a la hora de graficar

# Generamos el grafico mejorando la información mostrada
sns.clustermap(df,
               col_cluster = False,    # elimina el dendograma superior
               cmap = "Reds",         # paleta de colores
               standard_scale = 1)     # estandarizamos los datos de c/ columna
plt.show()
