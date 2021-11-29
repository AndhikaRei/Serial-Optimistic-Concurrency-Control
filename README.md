# Serial Optimistic Concurrency Control (OCC)

**Optimistic concurrency control** is a concurrency control method that assumes multiple transactions can frequently complete without interfering with each other. The method starts by reading all of the transactions assuming that no conflict will occur. After that, the transaction undergoes a validation process which then if successful, it will start the writing process.

Here is a breakdown of the phases:

- **Read Phase:** Executing the reading phase of the transaction.
- **Validation Phase:** This phase starts by checking for all the transactions that are commited. After that, it will validate the current transaction execution at the start timestamp until the last commit timestamp. If conflict then will rollback the transaction. If not, then commit the transaction and add it to the transaction manager.
- **Write Phase:** Executing the writing phase of the transaction.

## Getting Started

### Requirement

- Python 3

### How to Run

Assuming that python is already installed, the following command could be run in the terminal.

```
py occ.py
```

To change the transactions, edit the `main` function inside the `occ.py` file.

## Contribution

This repository is made to fulfill IF3140 Database Management project. Team members:

- 13519027 Haikal Lazuardi Fadil
- 13519043 Reihan Andhika Putra
- 13519083 Shaffira Alya Mevia
- 13519103 Bryan Rinaldo
- 13519159 Benidictus Galih Mahar Putra

## Reference

- [Optimistic Concurrency Control](https://github.com/mwhittaker/occ) by mwhittaker
