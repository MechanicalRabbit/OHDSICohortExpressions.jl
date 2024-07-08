# OHDSICohortExpressions.jl

*OHDSI Cohort Expressions is a re-implementation of OHDSI's Circe*

[![Zulip Chat][chat-img]][chat-url]
[![Open Issues][issues-img]][issues-url]
[![Apache License][license-img]][license-url]

This is a proof-of-concept implementation of a conversion from the JSON
cohort definitions used in the OHDSI ecosystem into an SQL transaction.

### Project Status

At this time, this implementation is able to convert all 797 cohorts
from PhenotypeLibrary v0.1 to generate SQL that works against Amazon
RedShift, Microsoft SQL Server, and PostgreSQL.

There are significant gaps in functionality. Many expressions available
in the JSON cohort definition have yet to be translated. In these cases,
an assertion error should arise. We have yet to write documentation,
perform code review, or construct regression tests. The API is in a
provisional form and very likely to change.

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
