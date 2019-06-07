import glob
import dask.dataframe as dd
import pandas as pd
import io
from zipfile import ZipFile
###########################################
#### DEFINICION DE VARAIBALES GLOBALES ####
###########################################

PATH_DATA = '/home/davidcparrar/Documents/Resources/exportaciones'

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

if __name__ == '__main__':
    # for imp in importaciones:
    #     df = read_zip_file(imp,'18')
    #     print(df)
    print(importaciones)
