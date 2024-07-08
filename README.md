# OHDSICohortExpressions.jl

*OHDSI Cohort Expressions is a re-implementation of OHDSI's Circe*

[![Zulip Chat][chat-img]][chat-url]
[![Open Issues][issues-img]][issues-url]
[![Apache License][license-img]][license-url]

This is an alpha-quality implementation of a conversion from the JSON
cohort definitions used in the OHDSI ecosystem into an SQL transaction.

### Project Status

This project has only implemented a subset of the OHDSI Circe format
which we are able to verify via the OHDSI PhentotypeLibrary. Other
permutations are left as an `assert` error. If you encounter one of
these, you could [open][issue-gap] a gap issue. We've not implemented
these permutations since it's important to have a regression test,
therefore providing an example cohort definition and results is helpful.

### Example Usage

First, load or generate a cohort definition in OHDSI Circe format.
In this example, we load the cohort definition from `demo/ex-10-2.json`,
which corresponds to [excercise 10.2][ex-10-2] from the Book of OHDSI.

```julia
cohort_definition = read("demo/ex-10-2.json", String)
```

Next, use `OHDSICohortExpressions.translate()` to convert this cohort
definition to a FunSQL query object.

```julia
using OHDSICohortExpressions: translate

q = translate(cohort_definition, cohort_definition_id = 1)
```

Run `DBInterface.connect()` to create a connection to an OMOP CDM database.
The arguments of `DBInterface.connect()` depend on the database engine and
connection parameters.  Consult FunSQL documentation for more information.

```julia
using FunSQL, DBInterface

db = DBInterface.connect(FunSQL.SQLConnection{ … }, … )
```

Execute the query to return the corresponding cohort.

```julia
using DataFrames

cr = DBInterface.execute(db, q)

df = DataFrame(cr)
```

[julia]: https://julialang.org/downloads/
[julia-call]: https://www.rdocumentation.org/packages/JuliaCall/versions/0.17.4
[ex-10-2]: https://ohdsi.github.io/TheBookOfOhdsi/Cohorts.html#exr:exerciseCohortsSql
[chat-img]: https://img.shields.io/badge/chat-julia--zulip-blue
[chat-url]: https://julialang.zulipchat.com/#narrow/stream/237221-biology-health-and-medicine
[issues-img]: https://img.shields.io/github/issues/MechanicalRabbit/OHDSICohortExpressions.jl.svg
[issues-url]: https://github.com/MechanicalRabbit/OHDSICohortExpressions.jl/issues
[license-img]: https://img.shields.io/badge/license-Apache-blue.svg
[license-url]: https://raw.githubusercontent.com/MechanicalRabbit/OHDSICohortExpressions.jl/master/LICENSE
[issue-gap]: https://github.com/MechanicalRabbit/OHDSICohortExpressions.jl/issues/new?assignees=&labels=Circe+Fields&projects=&template=implement-circe-query-permutation.md&title=Implement+Circe+Parameter%3A+...
