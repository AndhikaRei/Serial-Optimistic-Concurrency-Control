from typing import Callable, List 

from database import Database, OCCDatabase

class Transaction:
    """
    This is our representation of single transaction in OCC.

    Attributes:
    ----------
    txn : Callable[[OCCDatabase], None]
        The function that will be executed in the transaction.
    number : int
        The transaction number.
    start_timestamp : int
        The timestamp when the transaction started.
    local_db : OCCDatabase
        The local database of the transaction.
    """
    
    def __init__(self, db:Database, txn: Callable[[OCCDatabase], None], number:int=0 , start_timestamp:int=0) -> None:
        """
        Constructor of the OCC transaction.

        Parameters:
        -----------
        db: Database
            the local database  
        txn: Callable[[OCCDatabase], None]
            the function that will be executed in the transaction
        number: int
            the transaction number
        start_timestamp: int
            the timestamp when the transaction started
        """
        self.local_db = OCCDatabase(db)
        self.txn = txn
        self.number = number
        self.start_timestamp = start_timestamp
    
    def execute(self) -> None:
        """
        Execute the transaction.
        """
        self.txn(self.local_db)

# Example function to be executed in the transaction.
def read(ks: List[str]) -> Callable[[OCCDatabase], None]:
    """
    Read all data in database.

    Parameters:
    -----------
    ks: List[str]
        the list of keys to be read
    """
    def txn(db: OCCDatabase) -> None:
        for k in ks:
            db.read(k)
    return txn

def blindWrite(ks: List[str], val:int=0) -> Callable[[OCCDatabase], None]:
    """
    Write all data in database if its key in ks with val .

    Parameters:
    -----------
    ks: List[str]
        the list of keys to be written
    val: int
        the value to be written
    """
    def txn(db: OCCDatabase) -> None:
        for k in ks:
            db.write(k, val)
    return txn

def write(ks: List[str]) -> Callable[[OCCDatabase], None]:
    """
    Write all data in database if its key in ks with its value incremented by one.

    Parameters:
    -----------
    ks: List[str]
        the list of keys to be written
    """
    def txn(db: OCCDatabase) -> None:
        for k in ks:
            new_val = db.read(k) + 1
            db.write(k, new_val)
    return txn

read_txn = read
write_txn = write
blind_write_txn = blindWrite