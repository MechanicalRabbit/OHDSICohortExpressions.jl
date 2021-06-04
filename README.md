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

Following is an example given the `demo/ex-10-2.json` cohort. This
corresponds to [excercise 10.2][ex-10-2] from the Book of OHDSI.
Supported dialects are `:redshift`, `:sqlserver`, and `:postgresql`.

```julia
julia> cohort = read("demo/ex-10-2.json", String)

julia> using OHDSICohortExpressions: translate, Model

julia> model = Model(cdm_version=v"5.3.1", cdm_schema="cdm",
                     vocabulary_schema="vcb", results_schema="res",
                     target_schema="res", target_table="cohort");

julia> tsql = translate(cohort, dialect=:sqlserver, model=model,
                         cohort_definition_id=1);

julia> println(tsql)
```

The return value, `tsql` is a SQL string with a transaction that
populates the `cohort` table for cohort definition `1`.

### Usage from "R"

Using [JuliaCall][julia-call] library, one could call this function from
"R". First, one must install `JuliaCall`.

```R
> install.packages("JuliaCall")
```

You could then initialize the Julia environment, and install this
library to the Julia environment.

```R
> library("JuliaCall")
> julia_setup(installJulia = TRUE)
> julia_install_package_if_needed("OHDSICohortExpressions")
```

Construct an "R" proxy, `oce`, to the `Model` and `translate` functions.

```R
> oce <- julia_pkg_import("OHDSICohortExpressions",
                          func_list = c("Model", "translate"))
```

Construct a `model` object with data model parameters.

```R
> model = oce$Model(cdm_version="5.3.1", cdm_schema="cdm",
                    vocabulary_schema="vcb", results_schema="res",
                    target_schema="res", target_table="cohort")
```

Read the `cohort` file into an R variable.

```R
> library("readr")
> cohort <- read_file("demo/ex-10-2.json")
```

Translate this cohort definition into the SQL transaction.

```R
> tsql = oce$translate(cohort, dialect="sqlserver", model=model,
                       cohort_definition_id=1)
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
