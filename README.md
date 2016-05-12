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

---

### 1. Impute Slaughtered

The module imputes the animal and meat commodity and ensure the slaughtered
number in both commodity are identical.

**Inputs:**

* Production domain
* Animal to meat mapping table
* Flag table
* Yield formula

**Steps:**

1. Transfer Slaughtered animal from the animal commodity to the meat commodity.
2. Perform imputation on the meat commodity.
3. Transfer the imputed slaughtered animal back to the animal commodity.

**Flag Changes:**

| Procedure | Observation Status Flag | Method Flag|
| --- | --- | --- |
| Tranasfer between animal and meat commodity | `<Same as origin>` | `<Same as origin>` |
| Balance by Production Identity | I | i |
| Imputation | I | e |

**NOTE: The method flag for transfer is currently incorrectly implemented, it
  will be changed to "c"**

**Output:**

Meat commodity complete imputed where available and the slaughtered animal
synced between the meat and the parent commodity.

---

### 2. Synchronise Slaughtered

The module transfers the animal slaughtered from the animal commodity to all
related derivative such as skins, hide, and offals.

**Inputs:**

* Production domain
* Animal to meat mapping table
* Flag table
* Commodity tree table

**Steps:**


commodity.

**Flag Changes:**

| Procedure | Observation Status Flag | Method Flag|
| --- | --- | --- |
| Tranasfer between animal and meat commodity | `<Same as origin>` | `<Same as origin>` |

**NOTE: The method flag for transfer is currently incorrectly implemented, it
  will be changed to "c"**

**Output:**

Animal slaughtered in the animal commodity transfered to all derived products.

---

### 3. Balance Production Identity

This module ensures the production/area harvested/yield relationship is
fulfilled and calculate any missing values where available.

**Inputs:**

* Production domain
* Yield formula

**Steps:**

1. Compute yield
2. Balance Production
3. Balance Area Harvested

**Flag Changes:**

| Procedure | Observation Status Flag | Method Flag|
| --- | --- | --- |
| Compute/Balance | `<Flag Aggregation>` | i |

**Output:**

The production domain data balanced according to the equation.

---

### 4. Build Imputed Dataset

This module performs the imputation and saves the imputed values as `.rds`
objects back to the SWS shared drive. The purpose of this module is to avoid the
long running time of imputation, and allow the imputation to be loaded.

**Inputs:**

* Production domain
* Animal to meat mapping table
* Complete imputation key
* Yield formula

**Steps:**

1. Compute yield
2. Impute yield
3. Balance production
4. Impute production
5. Balance Area Harvested
6. Impute Area harvested
7. Compute yield
8. Impute yield

**NOTE: Step 7 and 8 are to ensure that yield are updated again according to the
  new imputation of production and area harvested. However, this does not ensure
  convergence and full complete imputation. A new algorithm is in development to
  ensure convergence and complete imputation where available.**

**Flag Changes:**

No flag change as the data is not saved back.

**Output:**

An `.rds` object saved on the SWS shared drive with all production imputed where
available.

---

### 5. Fill Imputation

This module loads the imputed value from the `.rds` file from the shared drive
and fill and save the missing values.

**Inputs:**

* Production domain
* Imputed `.rds` object
* Complete imputation key
* Yield formula

**Steps:**

1. Fill in imputed value from previous imputed dataset

**Flag Changes:**

| Procedure | Observation Status Flag | Method Flag |
| --- | --- | --- |
| Compute/Balance | `<Flag Aggregation>` | i |
| Imputation | I | e |

**Output:**

Production domain imputed where available.

---

**All work under this repository represents the latest status of development and
   is made public for collaboration purposes. It does not reflect the current
   state of the system and use of the program is at the discretion of the
   users.**
