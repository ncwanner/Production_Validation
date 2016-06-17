# Production Domain Overview

This is the repository containing both the package and the modules for
processing the production domain.

The repository also contains information on the complete processing cycle and
research materials.

---

## Production Domain Cycle

The complete cycle contains 4 stages:

### 1. Data collection

This phase collects data and inputs from various sources and merge them into
a single final information set.

No R module is involved in this phase.

### 2. Data validation

In this phase, the input data will be checked, corrected and validated prior
to imputation of missing data. All the process will be automised with
algorithms.

#### [R module: Production Input Validation](https://github.com/SWS-Methodology/faoswsProduction/tree/master/modules/production_input_validation)

This module performs both input validation of the production domain, and at
the same auto-correction of data with given rules.


### 3. Imputation

In this phase, the missing records will be imputed. All the process will be
automised with algorithms. The imputation is consists of two modules, each
module performs imputation on a different basket of commodities depending on the
nature. The module `Impute Livestock` performs imputation on the livestock item
while the module `Impute Non-livestock` operates on non-liveestock commodities.

#### [R module: Impute Livestock](https://github.com/SWS-Methodology/faoswsProduction/tree/master/modules/impute_livestock)

This module performs imputation on the livestock commodities and at the same
time ensure slaughtered animal is synchronised accross all related
parent/child commodities.

#### [R module: Impute Non-livestock](https://github.com/SWS-Methodology/faoswsProduction/tree/master/modules/impute_non_livestock)

This module craetes the imputed values for the non-livestock items.

### 4. Post validation

During this phase, the processed dataset will be investigated. It will be
possible to manually correct imputed values. However, all corrections are
required to be scientifically justified, mandatorily explained in the metadata,
and reported to team A for continual improvements of the algorithm.

After the manual intervention, the execution of the `Balance Production
Identity` is required to ensure the production is balanced.

#### [R module: Balance Production Identity](https://github.com/SWS-Methodology/faoswsProduction/tree/master/modules/balance_production_identity)

This module re-calculates the production identity, this ensures the
relationship of `Production = Area Harvested x Yield` holds when new changes
are introduced in the post validation phase.

![Production Work Flow](production_workflow_horizontal.jpg?raw=true "Work Flow")

---

## Auxiliary Datasets

The production modules in addition to the main production data
(`agriculture:aproduction`) depends on several auxiliary datasets detailed
below:

* `Complete Key Table`: This table specifies the scope of the data that will be
  processed. The table is a Datatable on the SWS as `fbs_production_comm_code`
  and can be extracted with the function `faoswsUtil:getCompleteImputationKey`.

* `Livestock Element Mapping Table`: This table provides the mapping of element
  codes between a parent commodity and child commodity. The table is currently
  on the Statistical Shared Drive under
  '/kao/production/animal_parent_child_mapping.csv'. A request is already sent
  to migrate the table into the system, please see issue
  [#128](https://github.com/SWS-Methodology/faoswsProduction/issues/128). The
  table can be accessed via the `getAnimalMeatMapping` function.

* `Identity Formula Table`: This table contains the triplet element code (input,
  output and productivity) for each commodity. The triplet is used to calculate
  the equation `output = input x productivity`. In the case of crops, it
  translates to `Production = area harvested x yield` where as in the case of
  meat it corresponds to `Production = Animal Slaughtered x Carcass Weight`.
  This is a DataTable on the system under the name `item_type_yield_elements`
  and can be retrieved with the function `getYieldFormula`.

* `Share table`: This dataset contains the share used to allocate parent
  commodities to child commodities. When transferring values from parent to
  child, `share` determines the proportion of the parent commodity is allocated
  to produce the child commodity. This is a Dataset on the SWS
  (`agriculture:aupus_share`) and can be accessed with the function
  `faoswsUtil:getShareData`.

## Production Processing

During the production imputation phase, certain values based on their
observation status and method flag will be removed and replaced with new
imputation values. Below we give a description of the flags being processed.

When considering processing production data, the method flag is the main flag
which determines the process. The following list provide a guideline for
processing the flags. In addition, the description is only valid for the dataset
'aproduction'.

* `-`: These represents data copied from the old system, they will not be replaced
   even in the case of (I, -) where the value is imputed. This is because they
   form a large part of the data and are acutally calculated on researched
   parameters. The removal of this flag will create massive disruption to the
   data and procedure.

* `q`: This is data collected from production questionnaire and they form part of
  the core data that are considered as accepted as fact and should not be
  modified unless iven valid justification.

* `p`: These are data collected from external database or publication manually.
  They are considered as part of data collection as should not be modified
  unless given valid justification.

* `h`: These are data harvested by the harvestor developed by the Engineering
  team, again, only valid sources are collected and can only be modified in
  exceptional cases.

* `c`: This flag was created by the old production modules where the values are
  copied in the synchronisation module. However, since the synchronisation
  involves calculation based on assumed relationship, they will be replaced with
  the calculated method flag 'i'.

* `b`: This flag only occurs at the FBS level, and thus should not appear in the
  aproduction dataset.

* `s`: Again, not part of the aproduction dataset.

* `t`: Not part of aproduction. This method flag is no longer valid.

* `i`: This flag represents data that has been calculated. They should be
  replaced everytime the production cycle is initiated.

* `e`: This flag is associated with values calculated by statistical algorithm,
  again, they should be replaced by the imputation cycle.

* `f`: This is a flag associated with manual estimated value, they may
  incorporate doomain expertise and accepted as a valid value under the Bayesian
  framework unless new data from questionnaie/external database,
  publication/harvestor are made available.

The following flag combination list are of particular interest to the processing
of production domain. Additional information are provided.


* `(E, e)`: This is invalid and will be corrected to (I, e)

* `(E, f)`: This is manual estimate, their presence in the new workflow of
            production should be limited. Nevertheless, they represent a piece
            of information and should not be removed during the production
            imputation phase.

* `(E, i)`: I am uncertain where this came from.

* `(E, p)`: This Estimates produced by external publication or database but
            collected manually. They will not be removed in the production
            imputation as it is assumed estimates from specialised agencies can
            provide better estimation/imputation.

* `(I, -)`: These are 'imputation' of the old system, or more like calculated
            based on shares/extraction rates or some relationship which the
            parameters may be obsolete. For consistentcy and minimise
            distruption, they will also not be removed from the system. Further,
            as they old system phase out, the count of this particular flag will
            become lesser and eventually has no effect on the general system.

* `(I, e)`: This is the imputation generated in the new statistical working
            system. For every new cycle of the production imputation phase, they
            will be removed and replaced with the new imputation.

* `(I, i)`: This is an invalid flag generated by the old method in the new
            statistical working system. Prior to the changes, balanced data were
            given the method flag 'i' and observation status flag "I" as they
            were generated by the imputation algorithm. However, the observation
            flag will now be replaced with flag aggregation. This flag will be
            removed and re-calculated in the imputation phase.

**All work under this repository represents the latest status of development and
   is made public for collaboration purposes. It does not reflect the current
   state of the system and use of the program is at the discretion of the
   users.**
