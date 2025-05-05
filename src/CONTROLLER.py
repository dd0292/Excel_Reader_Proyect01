from src import MODEL

import sys
import pandas as pd
from tqdm import tqdm
import time
import os

#----------------------------------------------------

def get_files_in_directory(directory_path: str):
    try:
        return [file for file in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, file))]
    
    except FileNotFoundError:
        print(f" [ERROR] The directory '{directory_path}' was not found...")
        return []
    
    except Exception as e:
        print(f"[ERROR] An error occurred: {e}")
        return []

def replace_in_all_strings(string_list, old_substring, new_substring):
    return [s.replace(old_substring, new_substring) for s in string_list]

def creat_list_of_tables_from_str_list(processor: MODEL.TableProcessor, string_list):
    tableList = []
    for ele in string_list:
        table = processor.GetTables(ele)
        tableList.append(table)
    return tableList

#----------------------------------------------------

def sub_add_PAIS(df, processor: MODEL.TableProcessor):
    centros_df = processor.GetTables('CENTROS')

    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={'PAIS': {
                'source_table': 'CENTROS',
                'join_on': 'CENTRO',
                'join_target': 'Centro',
                'source_column': 'PAIS_2'
            }
        },
        source_tables={'CENTROS': centros_df},
        save_to_db=False 
    )

def sub_add_CENTROS(df, processor: MODEL.TableProcessor):
    centros_df = processor.GetTables('CENTROS')

    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={'CENTROS': {
            'source_table': 'CENTROS',
            'join_on': 'CENTRO',
            'join_target': 'Centro', 
            'source_column': 'CENTRO_ID'       
            }
        },
        source_tables={'CENTROS': centros_df},
        save_to_db=False
    )

def sub_add_CANAL(df, processor: MODEL.TableProcessor):
    canal_df = processor.GetTables('CANAL')

    df = processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'CANAL': {
                'source_table': 'CANAL',
                'join_on': 'CANAL_ID',
                'join_target': 'Canal distribución',
                'source_column': 'CANAL_DESCRIP'
            }
        },
        source_tables={'CANAL': canal_df},
        save_to_db=False
    )

    df['CANAL'] = df.apply(
        lambda row: (
            "WHOLESALE DEFERRET"
            if str(row['Canal distribución']) == "20" and row['CENTROS'] == "G601"
            else row['CANAL']
        ),
        axis=1
    )
    return df

def sub_add_CLIENTE_DESCRPCION(df, processor: MODEL.TableProcessor):
    clientes_df = processor.GetTables('CLIENTES')

    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={'CLIENTE_DESCRPCION': {
            'source_table': 'CLIENTES',
            'join_on': 'Deudor',
            'join_target': 'Cliente', 
            'source_column': 'Nombre_1'       
            }
        },
        source_tables={'CLIENTES': clientes_df},
        save_to_db=False
    )

def sub_add_CLASIFICACION(df, processor: MODEL.TableProcessor):
    
    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'CLASIFICACION': lambda row: (
                "PROYECTOS B2B" if str(row["Canal distribución"]) == "30" and str(row["CENTROS"]) == "ZSER"
                else "DEFERRET" if str(row["Artículo"]).startswith(("10074", "956", "951"))
                else "PINTURA" if str(row["Artículo"]).startswith(("1", "2"))
                else "APLICADORES" if str(row["Artículo"]).startswith("0")
                else "MP" if str(row["Artículo"]).startswith("D")
                else "MERCADEO" if str(row["Artículo"]).startswith("8")
                else "LIQUIDACIÓN" if str(row["Artículo"]).startswith("9")
                else "EMPAQUE" if str(row["Artículo"])[0] in ["E", "L", "C", "A", "B", "F", "H", "I", "N", "T", "J"]
                else "SERVICIO" if str(row["Artículo"]).startswith("S")
                else "INSUMO" if str(row["Artículo"]).startswith(("O", "R"))
                else "DEFERRET"
            )
        },
        save_to_db=False
    )

def sub_add_FAMILIA(df, processor: MODEL.TableProcessor):
    
    return processor.AddColumns(
    df=df,
    table_name='VentaHistoricaTOTAL',
    column_definitions={
        'FAMILIA': lambda row: (
            "---" if True
            else "---"
        )
    },
    save_to_db=False
)

def sub_add_LINEA(df, processor: MODEL.TableProcessor):
    
    return processor.AddColumns(
    df=df,
    table_name='VentaHistoricaTOTAL',
    column_definitions={
        'LINEA': lambda row: (
            str(row['Artículo'])[:5]
        )
    },
    save_to_db=False
)

def sub_add_MATERIAL(df, processor: MODEL.TableProcessor):
    
    codigosCambian_df = processor.GetTables('CODIGOS_CAMBIAN')
    codigosCambian_df['CODIGO_SER'] = codigosCambian_df['CODIGO_SER'].astype(str).str.strip()

    df = processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={'MATERIAL': {
                'source_table': 'CODIGOS_CAMBIAN',
                'join_on': 'CODIGO_SER',
                'join_target': 'Artículo',  
                'source_column': 'CODIGO_PT'  
            }
        },
        source_tables={'CODIGOS_CAMBIAN': codigosCambian_df},
        save_to_db=False
    )

    df['MATERIAL'] = df.apply(
        lambda row: 
        row['Artículo'] if str(row['PAIS']) == "GUATEMALA" and str(row['CANAL']) == "RETAIL"
        else row['Artículo'] if pd.isna(row['MATERIAL']) 
        else row['MATERIAL'],
        axis=1
    )

    return df

def sub_add_SEGMENTO(df, processor: MODEL.TableProcessor):
    segmento1_df = processor.GetTables('SEGMENTO_CLIENTE')
    segmento2_df = processor.GetTables('SEGMENTO_CODIGO')

    df['Cliente'] = df['Cliente'].astype(str)
    df['MATERIAL'] = df['MATERIAL'].astype(str)
    segmento1_df['ID_CLIENTE'] = segmento1_df['ID_CLIENTE'].astype(str)
    segmento2_df['MATERIAL'] = segmento2_df['MATERIAL'].astype(str)

    df = df.merge(
        segmento1_df[['ID_CLIENTE', 'SEGMENTO']],
        how='left',
        left_on='Cliente',
        right_on='ID_CLIENTE'
    ).rename(columns={'SEGMENTO': 'SEG1'})

    df = df.merge(
        segmento2_df[['MATERIAL', 'SEGMENTO']],
        how='left',
        on='MATERIAL'
    ).rename(columns={'SEGMENTO': 'SEG2'})

    df['SEGMENTO'] = df['SEG1'].fillna(df['SEG2']).fillna(df['CLASIFICACION'])

    df.drop(columns=['SEG1', 'SEG2', 'ID_CLIENTE'], inplace=True)

    return df

def sub_add_DESCRIPTION(df, processor: MODEL.TableProcessor):
    mara_df = processor.GetTables('MARA')

    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'DESCRIPTION': {
            'source_table': 'MARA',
            'join_on': 'Material',
            'join_target': 'MATERIAL', 
            'source_column': 'Texto_breve_de_material'       
            }
        },

        source_tables={
            'MARA': mara_df
        },
        save_to_db= False
    )

def sub_add_VALIDACION_COD(df, processor: MODEL.TableProcessor):
    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'VALIDACION_COD': lambda row: (
                str(str(row['Artículo']) == str(row['MATERIAL']))
            )
        },
        save_to_db=False
    )

def sub_add_VOLUMEN(df, processor: MODEL.TableProcessor):
    mara_df = processor.GetTables('MARA')

    df = processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'VOLUMEN': {
            'source_table': 'MARA',
            'join_on': 'Material',
            'join_target': 'MATERIAL', 
            'source_column': 'Volumen'       
            }
        },

        source_tables={
            'MARA': mara_df
        },
        save_to_db=False
    )

    df['VOLUMEN'] = df.apply(
        lambda row: 
        0 if pd.isna(row['VOLUMEN']) or row['VOLUMEN'] == None or row['VOLUMEN'] == "None"
        else row['VOLUMEN'],
        axis=1
    )
    return df
    
def sub_add_UNIDADES(df, processor: MODEL.TableProcessor):
    walmart_df = processor.GetTables('WALMART_ESA_MASTER_PACK')

    df = processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'UNIDADES': {
                'source_table': 'WALMART_ESA_MASTER_PACK',
                'join_on': 'CODIGO_SAP',
                'join_target': 'MATERIAL',
                'source_column': 'MASTERPACK_COMERCIAL'
            }
        },
        source_tables={
            'WALMART_ESA_MASTER_PACK': walmart_df
        },
        save_to_db=False
    )

    def calcular_unidades(row):
        if row['MATERIAL'] == "NA":
            return 0
        elif row['PAIS'] == "GUATEMALA" and row['CANAL'] == "RETAIL" and row['LINEA'] == "500-0":
            return row['Volumen de ventas'] / 2063
        elif row['Cliente'] == "110004493":
            unidades_base = 0 if pd.isna(row['UNIDADES']) else row['UNIDADES']
            return unidades_base * row['Volumen de ventas']
        else:
            return row['Volumen de ventas']

    df['UNIDADES'] = df.apply(calcular_unidades, axis=1)

    return df

def sub_add_MONTO_USD(df, processor: MODEL.TableProcessor):
    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'MONTO_USD': lambda row: (
                0 if str(row["MATERIAL"]) ==  "NA"
                else row['Valor Neto']
            )
        }, 
        save_to_db=False
    )

def sub_add_GALONES(df, processor: MODEL.TableProcessor):
    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'GALONES': lambda row: (
                (row['UNIDADES'] if pd.notna(row['UNIDADES']) else 0) *
                (row['VOLUMEN'] if pd.notna(row['VOLUMEN']) else 0)
            )
        }, 
        save_to_db= False
    )

def sub_add_FILTRO1(df, processor: MODEL.TableProcessor):
    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'FILTRO1': lambda row: (
                "SI" if row['CLASIFICACION'] in ["PINTURA", "PROYECTOS B2B", "APLICADORES", "DEFERRET"]
                else "SI" if (
                    (str(row['PAIS']) if pd.notna(row['PAIS']) else '') +
                    (str(row['CLIENTE_DESCRPCION']) if pd.notna(row['CLIENTE_DESCRPCION']) else '') +
                    (str(row['MATERIAL']) if pd.notna(row['MATERIAL']) else '')
                ) in [
                    "TEGUCIPALPAGRUPO DEWARE S.AE000000000-01",
                    "TEGUCIPALPAGRUPO DEWARE S.AE000000025-05",
                    "TEGUCIPALPAGRUPO DEWARE S.AE000000000-04"
                ]
                else "NO"
            )
        },
        save_to_db=False
    )

def sub_add_FILTRO2(df, processor: MODEL.TableProcessor):
    tipoFacturas_df = processor.GetTables('TIPO_FACTURAS')

    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'FILTRO2': {
            'source_table': 'TIPO_FACTURAS',
            'join_on': 'TIPO_FACTURA',
            'join_target': 'Clase de factura', 
            'source_column': 'VENTA_BRUTA'       
            }
        },

        source_tables={
            'TIPO_FACTURAS': tipoFacturas_df
        },
        save_to_db=False

    )

def sub_add_FILTRO3(df, processor: MODEL.TableProcessor):
   
    return processor.AddColumns(
        df=df,
        table_name='VentaHistoricaTOTAL',
        column_definitions={
            'FILTRO3': lambda row: (
                "SI" if str(row['FILTRO1']) + str(row['FILTRO2']) == "SISI"
                else "NO"
            )
        },
        save_to_db=False
    )

#----------------------------------------------------

def _setUpDataBase(processor: MODEL.TableProcessor, excel_path: str):
    tablas = [
        "CANAL",
        "CENTROS",
        "SEGMENTO_CLIENTE",
        "SEGMENTO_CODIGO",
        "TIPO_FACTURAS",
        "MARA",
        "CODIGOS_CAMBIAN",
        "CLIENTES",
        "WALMART_ESA_MASTER_PACK"
    ]

    total = len(tablas)

    print("Iniciando carga de datos...\n")
    for i, tabla in enumerate(tablas, 1):
        processor.ImportFromExcel(excel_path, tabla, tabla)

        porcentaje = int(100 * i / total)
        barra = '#' * int(porcentaje / 2) + '-' * int((100 - porcentaje) / 2)
        sys.stdout.write(f'\rCargando: [{barra}] {porcentaje}% ')


    print("\n Todas las tablas han sido cargadas correctamente...")

#----------------------------------------------------

def _listDataBase(processor: MODEL.TableProcessor):
    for table in processor.ListTables():
        print(table)

def _printColumnsTableFromDataBase(processor: MODEL.TableProcessor):
    table_name = input("[Nombre]: ")
    processor.PrintColumns(table_name)

def _downloadExcelFromDataBase(processor: MODEL.TableProcessor):
    table_name = input("[Nombre]: ")
    
    output_path = input("[Direccion]: ")
    output_path = output_path if output_path != "" else None
    
    sheet_name =input("[Hoja]: ")
    sheet_name = sheet_name if sheet_name != "" else None


    processor.ExportToExcel(table_name,output_path,sheet_name)

def _printCompleteTableFromDataBase(processor: MODEL.TableProcessor):
    table_names = input("[Tabla]: ")
    print(processor.GetTables(table_names))

def _deleteTableFromDataBase(processor: MODEL.TableProcessor):
    table_names = input("[Tabla]: ")
    processor.DropTable(table_names)

def _pivoteTabels(processor: MODEL.TableProcessor):
    table_name = input("[Tabla]: ")
    processor.PrintColumns(table_name)
    table_name = processor.GetTables(table_name)

    index = input("[Index]: ").split()
    index = index if index != "" else None

    columns = input("[Columnas]: ").split()
    columns = columns if columns != "" else None

    values = input("[Valores]: ").split()
    values = values if values != "" else None
    
    output_table = input("[Nombre]: ")
    
    processor.PivotTables(table_name,output_table,index,columns,values)

#----------------------------------------------------

def _cargarVentaHistorica(processor: MODEL.TableProcessor):
    
    directoryPath = "Historial_de_Venta"
    filesList = get_files_in_directory(directoryPath)

    print(filesList)

    total = len(filesList)
    porcentaje = 0

    for i, table in enumerate(filesList, 1):

        barra = '#' * int(porcentaje / 2) + '-' * int((100 - porcentaje) / 2)
        sys.stdout.write(f'\rCargando: [{barra}] {porcentaje}% ')

        processor.ImportFromExcel(f"{directoryPath}\\{table}","Sheet1",table.replace(".XLSX","").replace("-","_"))
        
        porcentaje = int(100 * i / total)
        barra = '#' * int(porcentaje / 2) + '-' * int((100 - porcentaje) / 2)
        sys.stdout.write(f'\rCargando: [{barra}] {porcentaje}% ')

def _concatenateTabels(processor: MODEL.TableProcessor):

    directoryPath = "Historial_de_Venta"
    filesList = get_files_in_directory(directoryPath)

    filesList = replace_in_all_strings(filesList,".XLSX","")
    filesList = replace_in_all_strings(filesList,"-","_")
    filesList = creat_list_of_tables_from_str_list(processor,filesList)

    print(filesList[0])

    output_table = "VentaHistoricaTOTAL"

    processor.ConcatTables(filesList,output_table)

def _completarVentaHistorica(processor: MODEL.TableProcessor):
    table_name = 'VentaHistoricaTOTAL'
    df = processor.GetTables(table_name)

    steps = [
        ("PAIS", sub_add_PAIS),
        ("CENTRO", sub_add_CENTROS),
        ("CANAL", sub_add_CANAL),
        ("CLIENTE DESCRIPCION", sub_add_CLIENTE_DESCRPCION),
        ("CLASIFICACION", sub_add_CLASIFICACION),
        ("FAMILIA", sub_add_FAMILIA),
        ("LINEA", sub_add_LINEA),
        ("MATERIAL", sub_add_MATERIAL),
        ("SEGMENTO", sub_add_SEGMENTO),
        ("DESCRIPTION", sub_add_DESCRIPTION),
        ("VALIDACION_COD", sub_add_VALIDACION_COD),
        ("VOLUMEN", sub_add_VOLUMEN),
        ("UNIDADES", sub_add_UNIDADES),
        ("MONTO_USD", sub_add_MONTO_USD),
        ("GALONES", sub_add_GALONES),
        ("FILTRO1", sub_add_FILTRO1),
        ("FILTRO2", sub_add_FILTRO2),
        ("FILTRO3", sub_add_FILTRO3),
    ]

    print("\nProcesando columnas en VentaHistoricaTOTAL...\n")

    for label, func in tqdm(steps, desc="Completando columnas", unit="columna"):
        tqdm.write(f"▶ Ejecutando: {label}")
        df = func(df, processor)
        tqdm.write(f"✔ Completado: {label}")
        print("------------------------------------------------")
        time.sleep(0.1)

    # Save final result ONCE
    df.to_sql(table_name, processor.conn, if_exists='replace', index=False)
    print("\nProceso completado con éxito...\n")


def _pivotearDescargar_VENTA_BRUTA(processor: MODEL.TableProcessor):
    df = processor.GetTables('VentaHistoricaTOTAL')
    df = df[df['FILTRO3'] == 'SI']

    pivot_df = pd.pivot_table(
        df,
        index=['PAIS', 'CANAL', 'CLASIFICACION', 'SEGMENTO', 'FAMILIA', 'LINEA', 'CENTROS', 'MATERIAL', 'DESCRIPTION'],
        columns=['Período/Año'],
        values=['UNIDADES', 'MONTO_USD', 'GALONES'],
        aggfunc='sum',
        fill_value=0  
    )

    pivot_df.columns = ['{}_{}'.format(val, col) for val, col in pivot_df.columns]
    pivot_df = pivot_df.reset_index()

    processor.SetTable('pivot_result_BRUTA', pivot_df)
    print(pivot_df)
    processor.ExportToExcel(table_name='pivot_result_BRUTA', sheet_name="Sheet01")

def _pivotearDescargar_VENTA_NETA(processor: MODEL.TableProcessor):
    df = processor.GetTables('VentaHistoricaTOTAL')

    pivot_df = pd.pivot_table(
        df,
        index=['PAIS', 'CANAL', 'CLASIFICACION', 'SEGMENTO', 'FAMILIA', 'LINEA', 'CENTROS', 'MATERIAL', 'DESCRIPTION'],
        columns=['Período/Año'],
        values=['UNIDADES', 'MONTO_USD', 'GALONES'],
        aggfunc='sum',
        fill_value=0  
    )

    pivot_df.columns = ['{}_{}'.format(val, col) for val, col in pivot_df.columns]
    pivot_df = pivot_df.reset_index()

    processor.SetTable('pivot_result_NETA', pivot_df)
    print(pivot_df)
    processor.ExportToExcel(table_name='pivot_result_NETA', sheet_name="Sheet01")
