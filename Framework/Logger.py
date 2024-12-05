from datetime import date, datetime
import os


class Journal:
    def __init__(self):
        self

    def InsertToLog(self, meth: str, msg: str):
        if os.path.exists(".\\Log\\"):
            print("exists")
        else:
            os.makedirs(".\\Log\\")

        base_path: str = os.path.dirname(os.path.abspath(__file__))
        log_file: str = "\\Log\\" + str(date.today()) + ".log"
        with open(log_file, "a", encoding="utf_8") as f:
            print(f" {datetime.now()} | {meth}: --> {msg}", file=f)
