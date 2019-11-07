import glob
import dask.dataframe as dd
import pandas as pd
import io
from zipfile import ZipFile
###########################################
#### DEFINICION DE VARAIBALES GLOBALES ####
###########################################

PATH_DATA = '/home/davidcparrar/Documents/Resources/exportaciones'
columns_file = ['Año Gravable', 'Numero Periodo', 'Razon Social', 'Razón Social Destinatario',
                'Ciudad en el Exterior', 'Nombre Pais Destino','Codigo Pais Destino',
                'Valor Total Agregado Nacional USD','Número Total de Bultos',
                'Total Peso Bruto Kgs.','Codigo Subpartida','Valor FOB USD Subpartida',
                'Peso neto Kgs.', 'Peso bruto Kgs.']

todos = glob.glob(PATH_DATA+"/*")
importaciones = glob.glob(PATH_DATA+"/*500*")
exportaciones = [i for i in todos if i not in importaciones]

def read_zip_file(filename, partida):
    name = (importaciones[0].split('/')[-1]+'.xlsx').replace('.zip','')
    print('Archivo: {}'.format(name))
    df = pd.read_excel(io.BytesIO(ZipFile(filename).read(name)))
    print('Archivo: {}, Columnas: {}'.format(filename,len(df.columns)))

    subpartida = [col for col in df.columns if 'subpartida' in col.lower() and 'arance' in col.lower()]
    df = df[df[subpartida[0]].astype(str).str.startswith(partida)]
    return df.columns


def get_exportaciones(partida='18'):
    parts = [dask.delayed(pd.read_zip_file)(file_) for file_ in exportaciones]
    df = dd.from_delayed(parts)


def get_importaciones(partida='18'):
    df = dd.read_csv(importaciones)
    print(df.head(10))

def read_files_exportaciones(exportaciones,partida='18'):
    try:
        df = pd.read_excel(exportaciones,usecols=columns_file,nrows=10)
    except Exception as e:
        print('Could not read columns')
        df = pd.read_excel(exportaciones,nrows=10)

    try:
        df['Codigo Subpartida'] = df['Codigo Subpartida'].apply(lambda x: str(x))
        return df[df['Codigo Subpartida'].str.startswith(partida)]
    except Exception as e:
        print('No column Codigo Subpartida')
        return df

if __name__ == '__main__':
    for exp in exportaciones:
        print(exp)
        try:
            df =  read_files_exportaciones(exp)
            print(df.columns)
        except Exception as e:
            print('Could not process file: ', e)
