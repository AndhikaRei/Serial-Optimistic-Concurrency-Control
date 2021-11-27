# Import dependencies.

# Import own module
from database import *
from transaction import *
class TransactionExecutor:
    """
    Class that execute and validate OCC transaction in serial way.

    Attributes:
 
    """
    def __init__(self, db:'SerialDatabase', transaction:Transaction) -> None:
        """
        Constructor of the TransactionExecutor.

        Parameters:
        -----------
        db: SerialDatabase
            the local database
        transaction: Transaction
            The transaction to be executed.
        """
        self.db = db
        self.transaction = transaction

    def readingPhase(self) -> None:
        """
        Execute the reading phase of OCC transaction.
        """
        # Read the database
        self.transaction.execute()
    
    def validationAndWritingPhase(self) -> bool:
        """
        Execute the validation and writing phase of OCC transaction.
        """
        last_commit_timestamp = self.db.last_commit_timestamp
        # Check for all the transactions that are commited after the current transaction start.
        for timestamp in range(self.transaction.start_timestamp+1, last_commit_timestamp + 1):
            # Get the transaction local database that is commited at that timestamp. 
            if (self.db.transactions[timestamp] is not None):
                cached_db = self.db.transactions[timestamp].local_db
                # Validate with current transaction execution.
                # If conflict then rollback the transaction and return False.
                write_set = cached_db.write_set
                read_set = self.transaction.local_db.read_set
                if not write_set.isdisjoint(read_set):
                    print("Conflict detected write set: ", write_set," read set: ", read_set, 
                        "at timestamp ", timestamp, "between transaction no-",self.db.transactions[timestamp].number ,
                        " and transaction no-",  self.transaction.number )
                    return False
        # If not conflict then commit the transaction and add to transaction manager.
        self.writingPhase()
        return True
    
    def writingPhase(self) -> None:
        """
        Execute the writing phase of OCC transaction.
        """
        self.transaction.local_db.commit()
        self.db.commitTransaction(self.transaction)


class SerialDatabase(Database):
    """
    Class that manage OCC transaction execution.

    Attributes:
    -----------
    data: Database
        The database that is used for the transaction.
    transactions : Dict[int, Transaction]
        A dictionary that contains all the commited transactions that are currently in the transaction manager
    last_commit_timestamp : int
        The timestamp of the last commit.
    """
    def __init__(self) -> None:
        """
        Constructor of the SerialDatabase.
        """
        super().__init__()
        self.transactions: Dict[int, Transaction] = {}
        self.last_commit_timestamp: int = 0
    
    def commitTransaction(self, transaction: Transaction) -> None:
        """
        Commit a transaction.

        Parameters:
        -----------
        transaction: Transaction
            The transaction to be committed.
        """
        self.last_commit_timestamp += 1
        if (self.last_commit_timestamp in self.transactions):
            raise Exception("There are a transaction commited at that timestamp")
        self.transactions[self.last_commit_timestamp] = transaction
        transaction.local_db.commit()
        
    
    def begin(self, transaction: Transaction) -> TransactionExecutor:
        """
        Begin a transaction.
        
        Parameters:
        -----------
        transaction: Transaction
            The transaction to be executed.
        """
        return TransactionExecutor(self, transaction)

def main():
    occ = SerialDatabase()
    assert(occ.database == {})

    # Fill data/database with dummy value 0. 
    t0_txn = fill_txn(['a','b','c'])
    t0 = occ.begin(Transaction(occ, t0_txn, 0, 0))
    t0.readingPhase()
    assert(t0.validationAndWritingPhase())
    assert(occ.database == {'a': 0, 'b': 0, 'c': 0})
    assert(occ.last_commit_timestamp == 1)

    # Read data using two concurrent transactions.
    # Must be not conflict.
    t1_txn = read_txn(['a','b','c'])
    t2_txn = read_txn(['a','b','c'])
    t1 = occ.begin(Transaction(occ, t1_txn, 1, 1))
    t2 = occ.begin(Transaction(occ, t2_txn, 2, 1))
    t1.readingPhase()
    t2.readingPhase()
    assert(t1.validationAndWritingPhase())
    assert(occ.last_commit_timestamp == 2)
    assert(t2.validationAndWritingPhase())
    assert(occ.last_commit_timestamp == 3)

    # Write data using two concurrent transactions.
    # Must be conflict.
    t3_txn = write_txn(['a','b','c'])
    t4_txn = write_txn(['a','b','c'])
    t3 = occ.begin(Transaction(occ, t3_txn, 3, 3))
    t4 = occ.begin(Transaction(occ, t4_txn, 4, 3))
    t3.readingPhase()
    t4.readingPhase()
    assert(t3.validationAndWritingPhase())
    assert(occ.last_commit_timestamp == 4)
    assert(not t4.validationAndWritingPhase())
    assert(occ.last_commit_timestamp == 4)

    # Disjoin write set of two concurrent transactions.
    # Must be not conflict.
    t5_txn = write_txn(['a','b'])
    t6_txn = write_txn(['c'])
    t5 = occ.begin(Transaction(occ, t5_txn, 5, 4))
    t6 = occ.begin(Transaction(occ, t6_txn, 6, 4))
    t5.readingPhase()
    t6.readingPhase()
    assert(t5.validationAndWritingPhase())
    assert(occ.last_commit_timestamp == 5)
    assert(t6.validationAndWritingPhase())
    assert(occ.last_commit_timestamp == 6)

    # Dst

if __name__ == "__main__":
    main()