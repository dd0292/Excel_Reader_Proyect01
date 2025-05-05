from src import MODEL
from src import CONTROLLER
from src import VIEW

if __name__ == "__main__":

    Processor = MODEL.TableProcessor()

    if input("CARGAR DATOS NUEVOS [Y/N]: ").capitalize() == "Y":
        CONTROLLER._setUpDataBase(Processor, "Background.xlsx")

    VIEW.main_menu(Processor)
    Processor.close()