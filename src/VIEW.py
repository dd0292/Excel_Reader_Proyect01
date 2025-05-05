from src import CONTROLLER
from src import MODEL

def print_banner():
    print(f"""
╔══════════════════════════════════════════════════════════╗
                BIENVENIDO AL MENU DE DATOS           
╚══════════════════════════════════════════════════════════╝
""")

def print_menu():
    print(f"""
╔══════════════════════════════════════════════════════════╗
║                   OPCIONES DISPONIBLES                   ║
╠══════════════════════════════════════════════════════════╣
FUNCIONES:
    1. CARGAR VENTA HISTORICA
    2. CONCATENAR VENTA HISTORICA
    3. COMPLETAR VENTA HISTORICA
    4. PIVOTEAR & DESCARGAR [VENTA BRUTA]
    5. PIVOTEAR & DESCARGAR [VENTA NETA] 

CONTROL:
    6. TABLAS EN LA BASE DE DATOS
    7. VISUALISAR TABLA [COLUMNAS]
    8. VISUALISAR TABLA [COLUMNAS & DATOS]
    9. BORRAR TABLA
    10. DESCARGAR TABLA
    11. PIVOTEAR [MANUALMENTE]
╠══════════════════════════════════════════════════════════╣
    0. Salir del programa    
╚══════════════════════════════════════════════════════════╝
""")

def main_menu(processor: MODEL.TableProcessor):
    print_banner()

    while True:
        print_menu()
        choice = input(f"Ingrese una opción: ")

        if choice == "1":
            CONTROLLER._cargarVentaHistorica(processor)
        elif choice == "2":
            CONTROLLER._concatenateTabels(processor)
        elif choice == "3":
            CONTROLLER._completarVentaHistorica(processor)
        elif choice == "4":
            CONTROLLER._pivotearDescargar_VENTA_BRUTA(processor)
        elif choice == "5":
            CONTROLLER._pivotearDescargar_VENTA_NETA(processor)

        elif choice == "6":
            CONTROLLER._listDataBase(processor)
        elif choice == "7":
            CONTROLLER._printColumnsTableFromDataBase(processor)
        elif choice == "8":
            CONTROLLER._printCompleteTableFromDataBase(processor)
        elif choice == "9":
            CONTROLLER._deleteTableFromDataBase(processor)
        elif choice == "10":
            CONTROLLER._downloadExcelFromDataBase(processor)
        elif choice == "11":
            CONTROLLER._pivoteTabels(processor)

        elif choice == "0":
            processor.close()
            print(f"\n¡Hasta luego!")
            break
        
        else:
            print(f"Opción inválida. Intente de nuevo.")
