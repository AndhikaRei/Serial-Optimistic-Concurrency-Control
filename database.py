# Import dependency.
from typing import Dict, Any, Set

class Database:
    """
    This is our representation of database. It use dictionary that map a string to any value.
    Operation that happen in database is read and write.

    Attributes:
    ------------
    database: Dict[str, int]
        a dictionary that map a string to any value
    """

    def __init__(self) -> None:
        """
        Constructor of the database. Fill the database with empty dictionary.
        """
        self.database:Dict[str, int] = {}
        
    def read(self, key:str) -> Any:
        """
        Read the value of the key from the database. Return the value of the key or raise exception.

        Parameters:
        -----------
        key: str
            the key to be read
        """
        if key in self.database:
            return self.database[key]
        else:
            raise KeyError("Key not found")
    

    def write(self, key, value) -> None:
        """
        Write the value of the key to the database.
        
        Parameters:
        -----------
        key: str 
            the key to be written
        value: Any
            the value to be written
        """
        self.database[key] = value

class OCCDatabase:
    """
    This is our representation of private database used for every transaction. In OCC, every 
    transaction has its own local database and every change is applied to local database first.
    
    Attributes:
    database : Database
        The local database
    write_dict : Dict[str, int]
        It is used to keep track of the changes that are made in the local database.
    read_set : Set[str]
    """

    def __init__(self, db:Database = Database()) -> None:
        """
        Constructor of the OCC database.

        Parameters:
        -----------
        db: Database
            the local database
        """
        self.database:Database = db
        self.write_dict:Dict[str, int] = {}
        self.read_set:Set[str] = set()

    def read(self, key:str) -> Any:
        """
        Read the value of the key from the local database. Return the value of the key or raise exception.

        Parameters:
        -----------
        key: str
            the key to be read
        """
        if (self.database.read(key) != None):
            self.read_set.add(key)
            return self.database.read(key)
        raise KeyError("Key not found")

    def write(self, key:str, value:Any) -> None:
        """
        Write the value of the key to the local database.

        Parameters:
        -----------
        key: str 
            the key to be written
        value: Any
            the value to be written
        """
        self.write_dict[key] = value

    def commit(self) -> None:
        """
        Commit the changes made in the local database to the database.
        """
        for key in self.write_dict:
            self.database.write(key, self.write_dict[key])
        self.write_dict = {}
        self.read_set = set()
    
    @property
    def write_set(self) -> Set[str]:
        """
        Return the write set.
        """
        return set(self.write_dict.keys())