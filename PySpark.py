from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb


spark  = SparkSession.builder.appName('covid').getOrCreate()
datos_covid = spark.read.csv('/home/benjamin/Descargas/datos_abiertos_covid19_03.12.2021/211203COVID19MEXICO.csv',
                             header=True)


var_selec = datos_covid.select('ID_REGISTRO', 'ENTIDAD_RES','TIPO_PACIENTE', 'CLASIFICACION_FINAL', 'EDAD', 'SEXO', 'FECHA_DEF',
            'EMBARAZO','INTUBADO', 'NEUMONIA', 'DIABETES', 'EPOC', 'ASMA', 'INMUSUPR',
                   'HIPERTENSION', 'CARDIOVASCULAR', 'OBESIDAD', 'UCI')


#seleccion de los casos positivos
casos_positivos = var_selec[var_selec['CLASIFICACION_FINAL'] <= 3]

#mujeres embarazadas
mujeres_embarazadas_pos = casos_positivos[casos_positivos['EMBARAZO'] == 1]
mujeres_embarazadas_pos.head()

#abortos o muertes infantiles a edad 0
muertes_edad0 = casos_positivos[(casos_positivos['EDAD'] == 0) &
                                (casos_positivos['FECHA_DEF'] != '9999-99-99')]
def_edad0 = muertes_edad0.toPandas()

vector_busqueda = mujeres_embarazadas_pos['ID_REGISTRO'].isin(muertes_edad0['ID_REGISTRO'])
mujeres_embarazadas_pos = mujeres_embarazadas_pos.withColumn(colName = 'coincidencia',
                                                             col = vector_busqueda)

mujeres_abort = mujeres_embarazadas_pos[mujeres_embarazadas_pos['coincidencia'] == True]
mujeres_abort.head()

datos_mujeres_abort = mujeres_abort.toPandas()

datos_mujeres_abort.columns = ['id','ent','paci','clas','edad','sexo','defu','emba','intu',
                               'neum','diab', 'epoc', 'asma', 'inms','hipe','card',
                               'obes', 'uci', 'disc']


def_edad0['TIPO_PACIENTE'][def_edad0['TIPO_PACIENTE'] == '1'] = 'Ambulatorio'
def_edad0['TIPO_PACIENTE'][def_edad0['TIPO_PACIENTE'] == '2'] = 'Internado'

plt.figure()
plt.title('Pacientes de edad 0 difuntos')
sb.countplot(def_edad0['TIPO_PACIENTE'])
plt.xlabel('')
plt.ylabel('Casos')
plt.show()
plt.savefig('/home/benjamin/Escritorio/covid/tipo_pac.png')

def_edad0["UCI"][def_edad0['UCI'] == '1'] = "Si"
def_edad0["UCI"][def_edad0['UCI'] == '2'] = "No"
def_edad0["UCI"][def_edad0["UCI"] == '97'] = "No Aplica"

plt.figure()
plt.title('Cuidados intensivos edad 0 difuntos')
sb.countplot(def_edad0['UCI'])
plt.xlabel('')
plt.ylabel('Casos')
plt.show()
plt.savefig('/home/benjamin/Escritorio/covid/Cuidint.png')


entidades = ['01','02','03','04','05','06','07','08','09','10','11','12',
             '13','14','15','16','17','18','19','20','21','22','23','24',
             '25','26','27','28','29','30','31','32']
val_ent = ['AS','BC','BS','CC','CL','CM','CS','CH','DF','DG','GT','GR','HG',
           'JC','MC','MN','MS','NT','NL','OC','PL','QT','QR','SP','SL','SR',
           'TC','TS','TL','VZ','YN','ZS']

for i in range(len(entidades)):
    def_edad0['ENTIDAD_RES'][def_edad0['ENTIDAD_RES'] == entidades[i]] = val_ent[i]

plt.figure()
plt.title('Entidades con neonatales fallecidos por covid')
sb.countplot(def_edad0['ENTIDAD_RES'], order = val_ent)
plt.xlabel('')
plt.ylabel('Casos')
plt.show()
plt.savefig('/home/benjamin/Escritorio/covid/ent.png')



def_edad0["INTUBADO"][def_edad0['INTUBADO'] == '1'] = "Si"
def_edad0["INTUBADO"][def_edad0['INTUBADO'] == '2'] = "No"
def_edad0["INTUBADO"][def_edad0["INTUBADO"] == '97'] = "No Aplica"

plt.figure()
plt.title('Intubación edad 0 difuntos')
sb.countplot(def_edad0['INTUBADO'], order = ['Si','No','No Aplica'])
plt.xlabel('')
plt.ylabel('Casos')
plt.show()
plt.savefig('/home/benjamin/Escritorio/covid/intub.png')
