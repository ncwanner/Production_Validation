# sws_production

This is the repository containing both the package and the modules for
processing the production domain.

## Modules Overview

Production Module Flow:
![flowchart mm](https://cloud.githubusercontent.com/assets/1054320/15193239/18f489fc-17bd-11e6-9f3b-282c4891a702.png)

**NOTE: Module 1, 2, and 4 will be merged into a single module in the future, as
  the same logic applies.**

**NOTE: Module 3 which balance the production identity is no longer required in
  the sequence, it is a preliminary step in the imputation. The only use of this
  module is to balance the identity again when manual estimates are inputted
  after the imputation.**


### 1. Impute Slaughtered

**Input Datasets:**

* Production Domain
* Animal to meat mapping table
* Flag table
* Yield Formula

**Steps:**

1. Transfer Slaughtered animal from the animal commodity to the meat commodity.
2. Perform imputation on the meat commodity.
3. Transfer the imputed slaughtered animal back to the animal commodity.

**Flag Changes:**
------------------------------------------------------------------------
| Procedure | Observation Status Flag | Method Flag|
| --- | --- | --- |
| Tranasfer between animal and meat commodity | <Same as origin> | c |
| Balance by Production Identity | I | i |
| Imputation | I | e |
------------------------------------------------------------------------

**Output:**

Meat commodity complete imputed where available and the slaughtered animal
synced between the meat and the parent commodity.


### 2. Synchronise Slaughtered


### 3. Balance Production Identity


### 4. Build Imputed Dataset


### 5. Fill Imputation



**All work under this repository represents the latest status of development and
   is made public for collaboration purposes. It does not reflect the current
   state of the system and use of the program is at the discretion of the
   users.**
