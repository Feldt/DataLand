
class SQL():
    def __init__(self):
        self
        
    def ServerList(type: str) -> list:

        Server: dict = {        
            "TAG": [
                r'Server\Instance']           
        }

        return [value for (key, value) in Server.items() if key == type][0]