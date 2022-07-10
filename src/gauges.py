
import pandas as pd
import geopandas as gpd
import folium
import os
import sqlite3
import matplotlib.pylab as plt
from scipy import stats
import numpy as np

#para el ttest pvalue debe ser mayor a 0.05 para aceptar de que son la misma muestra
def tables_in_sqlite_db(conn):
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [
        v[0] for v in cursor.fetchall()
        if v[0] != "sqlite_sequence"
    ]
    cursor.close()
    return tables

def gauge_stations(df,cuencas_shp,this_map,cuenca_name):
    geometry=cuencas_shp[cuencas_shp.NOMBRE==cuenca_name]['geometry']
    def add_geo(geometry,this_map):
        sim_geo=gpd.GeoSeries(geometry).simplify(tolerance=0.001)
        geo_j= sim_geo.to_json()
        geo_j=folium.GeoJson(data=geo_j,style_function=lambda x: {'fillColor':'orange'})
        geo_j.add_to(this_map)
        
    def plotDot(df):
        popup_name=df.NOMBRE_ESTACION+' '+df.CATEGORIA
        folium.CircleMarker(location=[df.LATITUD,df.LONGITUD],popup=popup_name,radius=2).add_to(this_map)
        
    add_geo(geometry,this_map)
    df.apply(plotDot, axis = 1)
    
    return this_map

def find_index_row(df,values,col):
    idx=[]
    for val in values:
        item=df.index[df[col] == val].tolist()[0]
        idx.append(item)
    return idx

def clean_outliers_internos(df,categoria):
    if categoria =='convencional':
        cols = ['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']

        df[['Nivel Med']]=np.round(df[['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h']].mean(axis=1,skipna=True),2)

    elif categoria == 'automatica':
        cols = [f'Nivel {col_idx}h' for col_idx in range(24)]

    outlier_inter_dict={}
    for col in cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1

        outlier_upper=Q3 + 1.5 * IQR
        index_out=df[df[col]>outlier_upper].index.tolist()
        dates_out=df.loc[index_out,'Fecha Reg']
        outlier_inter_dict[col]=dates_out

    outlaiers_interno_df=pd.DataFrame(outlier_inter_dict)
    outlaiers_interno_solitarios=outlaiers_interno_df[outlaiers_interno_df.isnull().sum(axis=1)>2]

    for col in cols:
        elements_to_erase=find_index_row(df=df,col='Fecha Reg',values=outlaiers_interno_solitarios[col].dropna())
        df.loc[elements_to_erase,col]=np.NaN
        df.loc[df[col]<0,col]=np.NaN
        
    
    
    return df

class Estacion:
    def __init__(self,database):
        self.database = database
        sql_query= "SELECT * FROM Maestro"
        self.Maestro = pd.read_sql(sql_query, self.database)
        
        
        self.tables_clima={}
        

    def get_Maestro(self):
        return self.Maestro

    def estaciones_por_dz(self,num):
        self.tables_dz={}
        tables = tables_in_sqlite_db(self.database)
        filtro= self.Maestro[self.Maestro.DZ==num]
        name=list(filtro.NOMBRE_ESTACION+' '+filtro.CATEGORIA)
        
        for i,estacion in enumerate(filtro.CODIGO):
            estacion='T'+str(estacion)
            if estacion in tables:
                sql_query= "SELECT * FROM "+estacion
                df = pd.read_sql(sql_query, self.database)
                df.columns=['Codigo','Estacion','Fecha Reg','ano','mes','dia','Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med','Caudal']
                df[['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']]=df[['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']].apply(pd.to_numeric,errors='coerce')
                self.tables_dz[name[i]]=clean_outliers_internos(df,categoria='convencional')
                
        return self.tables_dz
    
    def estaciones_por_cuenca_dz(self,num):
        self.gauge_cuenca={}
        self.cod_gauge_cuenca={}
        
        tables = tables_in_sqlite_db(self.database)
        filtro= self.Maestro[self.Maestro.DZ==num]
        for cuenca in filtro.Cuenca:
            self.gauge_cuenca[cuenca]=[]
            self.cod_gauge_cuenca[cuenca]=[]
            
        cuencas=list(filtro.Cuenca)
        for i,estacion in enumerate(filtro.CODIGO):
            estacion_table='T'+str(estacion)
            if estacion_table in tables:
                cuenca=cuencas[i]
                sql_query= "SELECT * FROM "+estacion_table
                df=pd.read_sql(sql_query, self.database)
                df.columns=['Codigo','Estacion','Fecha Reg','ano','mes','dia','Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med','Caudal']
                df[['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']]=df[['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']].apply(pd.to_numeric,errors='coerce')
                self.gauge_cuenca[cuenca].append(df)
                self.cod_gauge_cuenca[cuenca].append(estacion)
            
        return (self.cod_gauge_cuenca,self.gauge_cuenca)
        
class Convencional(Estacion):
    def __init__(self,*args,**kwargs):
        super(Convencional,self).__init__(*args,**kwargs)
        
    
    def plot_dz_niveles(self):
        for estacion in self.tables_dz:
            df = self.tables_dz[estacion]
            df.columns=['Codigo','Estacion','Fecha Reg','ano','mes','dia','Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med','Caudal']
            df[['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']]=df[['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']].apply(pd.to_numeric,errors='coerce')
            df.plot(x='Fecha Reg',y=['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med'],figsize = (25,10),title=estacion)
        plt.show()
        
    def stats_dz_niveles(self):
        for estacion in self.tables_dz:
            df = self.tables_dz[estacion]
            print(estacion)
            print(df[['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']].describe())
    
    
    def max_historic_and_previous(self):
        self.stats_previous_historic=[]
        for estacion in self.tables_dz:
            df=self.tables_dz[estacion]
            df.columns=['Codigo','Estacion','Fecha Reg','ano','mes','dia','Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med','Caudal']
            
            cols=['Nivel 06h','Nivel 10h','Nivel 14h','Nivel 18h','Nivel Med']
            df[cols]=df[cols].apply(pd.to_numeric,errors='coerce')
            
            #Todos los anos presentes en la base de datos
            idx_years=pd.DatetimeIndex(df['Fecha Reg']).year
            years=np.unique(idx_years)
            if len(years)>=2:
                year0=years[0]
                previous_year=years[-2]
                present_year=years[-1]

                #year evaluation
                df_previous=df[idx_years==previous_year]
                df_historic=df[idx_years!=present_year]

                #month evaluation
                month_previous=pd.DatetimeIndex(df_previous['Fecha Reg']).month
                avenida_previous=(month_previous==1) | (month_previous==2) | (month_previous==3) | (month_previous==4)
                estiaje_previous=(month_previous==6) | (month_previous==7) | (month_previous==8) | (month_previous==9)

                month_historic = pd.DatetimeIndex(df_historic['Fecha Reg']).month
                avenida_historic=(month_historic==1) | (month_historic==2) | (month_historic==3) | (month_historic==4)
                estiaje_historic=(month_historic==6) | (month_historic==7) | (month_historic==8) | (month_historic==9)


                Estacion_niveles= [estacion+' '+col for col in cols]

                stats_df={}
                stats_df['Estaciones']=Estacion_niveles
                #stats_df['max año previo '+str(previous_year)]=list(df_previous[cols].max())
                #stats_df['max historico '+str(year0)+'-'+str(previous_year)]=list(df_historic[cols].max())
                
                stats_df['Año de Inicio']=list(np.repeat(year0,5))
                stats_df['Año de Fin']=list(np.repeat(present_year,5))
                stats_df['max año previo']=list(df_previous[cols].max())
                stats_df['max historico']=list(df_historic[cols].max())

                stats_df['max año previo estiaje']=df_previous[estiaje_previous][cols].max()
                stats_df['max año previo avenida']=df_previous[avenida_previous][cols].max()

                stats_df['max año historico estiaje']=list(df_historic[estiaje_historic][cols].max())
                stats_df['max año historico avenida']=list(df_historic[avenida_historic][cols].max())
                self.stats_previous_historic.append(pd.DataFrame(stats_df))
            else:
                print(estacion+ 'tiene menos de dos años de registro '+str(years))        
        return self.stats_previous_historic
    
# Las fechas de outliers que por lo menos no tenga una estacion vecina que lo confirme se elimino
# debe haber un control interno y de vecinos inicialmente
    
class Automatica(Estacion):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

    def estaciones_por_dz(self,num):
        self.tables_dz={}
        tables = tables_in_sqlite_db(self.database)
        filtro= self.Maestro[self.Maestro.DZ==num]
        name=list(filtro.NOMBRE_ESTACION+' '+filtro.CATEGORIA)
        for i,estacion in enumerate(filtro.CODIGO):
            estacion='T'+str(estacion)
            
            if estacion in tables:
                sql_query= "SELECT * FROM "+estacion
                df = pd.read_sql(sql_query, self.database)
                self.tables_dz[name[i]]=clean_outliers_internos(df,categoria='automatica')
                
        return self.tables_dz

    def estaciones_por_cuenca_dz(self,num):
        self.gauge_cuenca={}
        self.cod_gauge_cuenca={}
        
        tables = tables_in_sqlite_db(self.database)
        filtro= self.Maestro[self.Maestro.DZ==num]
        for cuenca in filtro.Cuenca:
            self.gauge_cuenca[cuenca]=[]
            self.cod_gauge_cuenca[cuenca]=[]
            
        cuencas=list(filtro.Cuenca)
        for i,estacion in enumerate(filtro.CODIGO):
            estacion_table='T'+str(estacion)
            if estacion_table in tables:
                cuenca=cuencas[i]
                sql_query= "SELECT * FROM "+estacion_table
                df=pd.read_sql(sql_query, self.database)
                self.gauge_cuenca[cuenca].append(df)
                self.cod_gauge_cuenca[cuenca].append(estacion)
            
        return (self.cod_gauge_cuenca,self.gauge_cuenca)

    def plot_dz_niveles(self):
        for estacion in self.tables_dz:
            df = self.tables_dz[estacion]
            df.plot(x='Fecha Reg',y=[f'Nivel {col_idx}h' for col_idx in range(24)],figsize = (25,10),title=estacion)
        plt.show()


    def max_historic_and_previous(self):
        self.stats_previous_historic=[]
        for estacion in self.tables_dz:
            df=self.tables_dz[estacion]
    
            cols=[f'Nivel {col_idx}h' for col_idx in range(24)]
           
            
            #Todos los anos presentes en la base de datos
            idx_years=pd.DatetimeIndex(df['Fecha Reg']).year
            years=np.unique(idx_years)
            if len(years)>=2:
                year0=years[0]
                previous_year=years[-2]
                present_year=years[-1]

                #year evaluation
                df_previous=df[idx_years==previous_year]
                df_historic=df[idx_years!=present_year]

                #month evaluation
                month_previous=pd.DatetimeIndex(df_previous['Fecha Reg']).month
                avenida_previous=(month_previous==1) | (month_previous==2) | (month_previous==3) | (month_previous==4)
                estiaje_previous=(month_previous==6) | (month_previous==7) | (month_previous==8) | (month_previous==9)

                month_historic = pd.DatetimeIndex(df_historic['Fecha Reg']).month
                avenida_historic=(month_historic==1) | (month_historic==2) | (month_historic==3) | (month_historic==4)
                estiaje_historic=(month_historic==6) | (month_historic==7) | (month_historic==8) | (month_historic==9)


                Estacion_niveles= [estacion+' '+col for col in cols]

                stats_df={}
                stats_df['Estaciones']=Estacion_niveles
                #stats_df['max año previo '+str(previous_year)]=list(df_previous[cols].max())
                #stats_df['max historico '+str(year0)+'-'+str(previous_year)]=list(df_historic[cols].max())
                
                stats_df['Año de Inicio']=list(np.repeat(year0,len(cols)))
                stats_df['Año de Fin']=list(np.repeat(present_year,len(cols)))
                stats_df['max año previo']=list(df_previous[cols].max())
                stats_df['max historico']=list(df_historic[cols].max())

                stats_df['max año previo estiaje']=df_previous[estiaje_previous][cols].max()
                stats_df['max año previo avenida']=df_previous[avenida_previous][cols].max()

                stats_df['max año historico estiaje']=list(df_historic[estiaje_historic][cols].max())
                stats_df['max año historico avenida']=list(df_historic[avenida_historic][cols].max())
                self.stats_previous_historic.append(pd.DataFrame(stats_df))
            else:
                print(estacion+ 'tiene menos de dos años de registro '+str(years))        
        return self.stats_previous_historic