using JSON
using Dates
using PrettyPrinting
using FunSQL:
    FunSQL, Agg, Append, As, Bind, Define, From, Fun, Get, Group, Join, LeftJoin,
    Partition, Select, Var, Where, render, SQLClause, SQLNode, SQLTable

FunSQL.arity(::Val{:extract_year}) = 1:1

function FunSQL.serialize!(::Val{:extract_year}, args::Vector{SQLClause}, ctx)
    if ctx.dialect.name === :sqlserver
        FunSQL.@serialize! "year" args ctx
    else
        FunSQL.@serialize! "EXTRACT(YEAR FROM ?)" args ctx
    end
end

FunSQL.arity(::Val{:dateadd_day}) = 2:2

function FunSQL.serialize!(::Val{:dateadd_day}, args::Vector{SQLClause}, ctx)
    if ctx.dialect.name === :sqlserver
        FunSQL.@serialize! "dateadd(day, ?, ?)" [args[2], args[1]] ctx
    else
        FunSQL.@serialize! "+" args ctx
    end
end

FunSQL.arity(::Val{:datediff_day}) = 2:2

function FunSQL.serialize!(::Val{:datediff_day}, args::Vector{SQLClause}, ctx)
    if ctx.dialect.name === :sqlserver
        FunSQL.@serialize! "datediff(day, ?, ?)" [args[2], args[1]] ctx
    else
        FunSQL.@serialize! "-" args ctx
    end
end

translate(cohort; dialect, model = Model(), cohort_definition_id) =
    translate(cohort, dialect, model, cohort_definition_id)

translate(cohort::String, dialect, model, cohort_definition_id) =
    translate(JSON.parse(cohort), dialect, model, cohort_definition_id)

function translate(cohort::Dict, dialect, model, cohort_definition_id)
    cohort = deepcopy(cohort)
    res = translate(unpack!(cohort), dialect, model, cohort_definition_id)
    @assert isempty(cohort)
    return res
end

struct TranslateContext
    dialect::Symbol
    model::Model
    cohort::CohortExpression
    cohort_definition_id::Int
    group_idx::Ref{Int}
    statements::Vector{String}
    temp_idx::Ref{Int}
    temp_map::Dict{SQLNode, SQLNode}
end

function next_group_alias(ctx::TranslateContext)
    alias = Symbol(:group_, ctx.group_idx[])
    ctx.group_idx[] += 1
    alias
end

function as_temp_table(ctx::TranslateContext, q)
    if q in keys(ctx.temp_map)
        return ctx.temp_map[q]
    end
    temp_table_name = Symbol(:temp_, ctx.temp_idx[])
    if ctx.dialect === :sqlserver
        temp_table_name = Symbol("#", temp_table_name)
    end
    ctx.temp_idx[] += 1
    key = q
    q = q |>
        Select(:person_id => Get.person_id,
               :event_id => Get.event_id,
               :start_date => Get.start_date,
               :end_date => Get.end_date,
               :sort_date => Get.sort_date,
               :op_start_date => Get.op_start_date,
               :op_end_date => Get.op_end_date,
               :visit_occurrence_id => Get.visit_occurrence_id)
    sql = render(q, dialect = ctx.dialect)
    sql =
        if ctx.dialect === :sqlserver
            "SELECT person_id, event_id, start_date, end_date, sort_date, op_start_date, op_end_date, visit_occurrence_id \nINTO $temp_table_name\nFROM (\n$sql\n) AS t;\n"
        else
            "CREATE TEMP TABLE $temp_table_name AS\n$sql;\n"
        end
    push!(ctx.statements, "DROP TABLE IF EXISTS $temp_table_name;\n")
    push!(ctx.statements, sql)
    val = From(SQLTable(temp_table_name, columns = [:person_id, :event_id, :start_date, :end_date, :sort_date, :op_start_date, :op_end_date, :visit_occurrence_id]))
    ctx.temp_map[key] = val
    val
end

function translate(cohort::CohortExpression, dialect, model, cohort_definition_id)
    ctx = TranslateContext(Symbol(dialect), model, cohort, cohort_definition_id, Ref(1), String[], Ref(1), Dict{SQLNode, SQLNode}())
    translate(cohort, ctx)
    join(ctx.statements)
end

function escape_name(dialect, name)
    if dialect === :sqlserver
        return "[$(replace(string(name), ']' => "]]"))]"
    else
        return "\"$(replace(string(name), '"' => "\"\""))\""
    end
end

function cohort_table(ctx::TranslateContext)
    cohort = ctx.model.cohort
    d = ctx.dialect
    if isnothing(cohort.schema) || "" == string(cohort.schema)
        return escape_name(d, cohort.name)
    end
    escape_name(d, cohort.schema) * "." * escape_name(d, cohort.name)
end

function translate(c::CohortExpression, ctx::TranslateContext)
    @assert c.censor_window.start_date === c.censor_window.end_date === nothing
    @assert isempty(c.censoring_criteria)
    @assert c.end_strategy === nothing || c.end_strategy isa DateOffsetStrategy
    cname = cohort_table(ctx)
    push!(ctx.statements, "DELETE FROM $(cname)\nWHERE cohort_definition_id = $(ctx.cohort_definition_id);\n")
    q = translate(c.primary_criteria, ctx)
    if (c.additional_criteria !== nothing && !isempty(c.additional_criteria)) || !isempty(c.inclusion_rules)
        q = base = as_temp_table(ctx, q)
    end
    if c.additional_criteria !== nothing && !isempty(c.additional_criteria)
        q = q |>
            translate(c.additional_criteria, ctx, base = base)
        q = q |>
            translate(c.qualified_limit, ctx)
    end
    for r in c.inclusion_rules
        q = q |>
            translate(r.expression, ctx, base = base)
    end
    q = q |>
        translate(c.expression_limit, ctx)
    if c.end_strategy !== nothing
        q = q |>
            translate(c.end_strategy, ctx)
    else
        q = q |>
            Define(:end_date => Get.op_end_date)
    end
    q = q |>
        translate(c.collapse_settings, ctx)
    q = q |>
        Select(:subject_id => Get.person_id,
               :cohort_start_date => Get.start_date,
               :cohort_end_date => Get.end_date)
    q = q |> Select(:cohort_definition_id => ctx.cohort_definition_id,
                    Get.subject_id,
                    Get.cohort_start_date,
                    Get.cohort_end_date)
    sql = render(q, dialect = ctx.dialect)
    cname = cohort_table(ctx)
    push!(ctx.statements, "INSERT INTO $cname\n$sql;\n")
end

translate(::Nothing, ctx::TranslateContext; base = nothing) =
    Define()

function translate(r::ResultLimit, ctx::TranslateContext; order_by = [Get.start_date])
    if r.type == ALL
        return Define()
    end
    if r.type == LAST
        order_by = [Fun.datediff_day(order_by[1], Date(2020, 1, 1)), order_by[2:end]...]
    end
    Partition(Get.person_id, order_by = order_by) |>
    Where(Agg.row_number() .== 1)
end

function translate(d::DateOffsetStrategy, ctx::TranslateContext)
    field =
        d.date_field == START_DATE ? Get.start_date :
        d.date_field == END_DATE ? Get.end_date :
        nothing
    Define(:end_date => dateadd_day(field, d.offset)) |>
    Define(:end_date => Fun.case(Get.end_date .<= Get.op_end_date,
                                 Get.end_date, Get.op_end_date))
end

function dateadd_day(n, delta::Integer)
    if iszero(delta)
        return n
    end
    Fun.dateadd_day(n, delta)
end

function translate(c::PrimaryCriteria, ctx::TranslateContext)
    @assert length(c.criteria_list) >= 1
    q = translate(c.criteria_list[1], ctx)
    if length(c.criteria_list) > 1
        args = [translate(l, ctx) for l in c.criteria_list[2:end]]
        q = q |>
            Append(args = args)
    end
    q = q |>
        Join(:op => ctx.model.observation_period |>
                    Define(:start_date => Get.observation_period_start_date,
                           :end_date => Get.observation_period_end_date),
             Get.person_id .== Get.op.person_id)
    q = q |>
        Define(:op_start_date => Get.op.start_date,
               :op_end_date => Get.op.end_date)
    l = dateadd_day(Get.op.start_date, c.observation_window.prior_days)
    r = dateadd_day(Get.op.end_date, - c.observation_window.post_days)
    q = q |>
        Where(Fun.and(l .<= Get.start_date, Get.start_date .<= r))
    q = q |>
        translate(c.primary_limit, ctx, order_by = [Get.sort_date, Get.event_id])
    q
end

function translate(c::ConditionOccurrence, ctx::TranslateContext)
    @assert isempty(c.condition_status)
    @assert !c.condition_type_exclude
    @assert c.stop_reason === nothing
    q = From(ctx.model.condition_occurrence) |>
        Define(:concept_id => Get.condition_concept_id,
               :event_id => Get.condition_occurrence_id,
               :start_date => Get.condition_start_date,
               :end_date => Fun.coalesce(Get.condition_end_date,
                                         dateadd_day(Get.condition_start_date, 1)),
               :sort_date => Get.condition_start_date)
    if c.condition_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.condition_source_concept_id,
                         translate(find_concept_set(c.condition_source_concept, ctx), ctx)))
    end
    if !isempty(c.condition_type)
        args = [Get.condition_type_concept_id .== t.concept_id
                for t in c.condition_type]
        q = q |>
            Where(Fun.or(args = args))
    end
    translate(c.base, ctx, base = q)
end

function translate(d::Death, ctx::TranslateContext)
    @assert isempty(d.death_type)
    @assert !d.death_type_exclude
    q = From(ctx.model.death) |>
        Define(:concept_id => Get.cause_concept_id,
               :event_id => 0,
               :start_date => Get.death_date,
               :end_date => dateadd_day(Get.death_date, 1),
               :sort_date => Get.death_date,
               :visit_occurrence_id => 0)
    if d.death_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.cause_source_concept_id,
                         translate(find_concept_set(d.death_source_concept, ctx), ctx)))
    end
    translate(d.base, ctx, base = q)
end

function translate(d::DeviceExposure, ctx::TranslateContext)
    @assert isempty(d.device_type)
    @assert !d.device_type_exclude
    @assert d.quantity === nothing
    @assert d.unique_device_id === nothing
    q = From(ctx.model.device_exposure) |>
        Define(:concept_id => Get.device_concept_id,
               :event_id => Get.device_exposure_id,
               :start_date => Get.device_exposure_start_date,
               :end_date => Fun.coalesce(Get.device_exposure_end_date,
                                         dateadd_day(Get.device_exposure_start_date, 1)),
               :sort_date => Get.device_exposure_start_date)
    if d.device_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.device_source_concept_id,
                         translate(find_concept_set(d.device_source_concept, ctx), ctx)))
    end
    translate(d.base, ctx, base = q)
end

function translate(d::DrugEra, ctx::TranslateContext)
    @assert d.era_start_date === nothing
    @assert d.era_end_date === nothing
    @assert d.occurrence_count === nothing
    @assert d.age_at_start === nothing
    @assert d.age_at_end === nothing
    q = From(ctx.model.drug_era) |>
        Define(:concept_id => Get.drug_concept_id,
               :event_id => Get.drug_era_id,
               :start_date => Get.drug_era_start_date,
               :end_date => Get.drug_era_end_date,
               :sort_date => Get.drug_era_start_date,
               :visit_occurrence_id => 0)
    if d.era_length !== nothing
        field = Fun.datediff_day(Get.drug_era_end_date, Get.drug_era_start_date)
        q = q |>
            Where(predicate(d.era_length, ctx, field = field))
    end
    translate(d.base, ctx, base = q)
end

function translate(d::DrugExposure, ctx::TranslateContext)
    @assert isempty(d.drug_type)
    @assert !d.drug_type_exclude
    @assert d.refills === nothing
    @assert d.quantity === nothing
    @assert d.days_supply === nothing
    @assert isempty(d.route_concept)
    @assert d.effective_drug_dose === nothing
    @assert isempty(d.dose_unit)
    @assert d.lot_number === nothing
    @assert d.stop_reason === nothing
    q = From(ctx.model.drug_exposure) |>
        Define(:concept_id => Get.drug_concept_id,
               :event_id => Get.drug_exposure_id,
               :start_date => Get.drug_exposure_start_date,
               :end_date => Fun.coalesce(Get.drug_exposure_end_date,
                                         Fun.dateadd_day(Get.drug_exposure_start_date, Get.days_supply),
                                         dateadd_day(Get.drug_exposure_start_date, 1)),
               :sort_date => Get.drug_exposure_start_date)
    if d.drug_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.drug_source_concept_id,
                         translate(find_concept_set(d.drug_source_concept, ctx), ctx)))
    end
    translate(d.base, ctx, base = q)
end

function translate(m::Measurement, ctx::TranslateContext)
    @assert isempty(m.measurement_type)
    @assert !m.measurement_type_exclude
    @assert m.abnormal === nothing
    @assert m.range_low === nothing
    @assert m.range_high === nothing
    @assert m.range_low_ratio === nothing
    @assert m.range_high_ratio === nothing
    @assert isempty(m.operator)
    q = From(ctx.model.measurement) |>
        Define(:concept_id => Get.measurement_concept_id,
               :event_id => Get.measurement_id,
               :start_date => Get.measurement_date,
               :end_date => dateadd_day(Get.measurement_date, 1),
               :sort_date => Get.measurement_date)
    if m.measurement_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.measurement_source_concept_id,
                         translate(find_concept_set(m.measurement_source_concept, ctx), ctx)))
    end
    if !isempty(m.value_as_concept)
        args = [Get.value_as_concept_id .== v.concept_id
                for v in m.value_as_concept]
        q = q |>
            Where(Fun.or(args = args))
    end
    if m.value_as_number !== nothing
        q = q |>
            Where(predicate(m.value_as_number, ctx, field = Get.value_as_number))
    end
    if !isempty(m.unit)
        args = [Get.unit_concept_id .== u.concept_id
                for u in m.unit]
        q = q |>
            Where(Fun.or(args = args))
    end
    translate(m.base, ctx, base = q)
end

function translate(o::Observation, ctx::TranslateContext)
    @assert isempty(o.observation_type)
    @assert !o.observation_type_exclude
    @assert o.value_as_string === nothing
    @assert isempty(o.qualifier)
    q = From(ctx.model.observation) |>
        Define(:concept_id => Get.observation_concept_id,
               :event_id => Get.observation_id,
               :start_date => Get.observation_date,
               :end_date => dateadd_day(Get.observation_date, 1),
               :sort_date => Get.observation_date)
    if o.observation_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.observation_source_concept_id,
                         translate(find_concept_set(o.observation_source_concept, ctx), ctx)))
    end
    if !isempty(o.value_as_concept)
        args = [Get.value_as_concept_id .== v.concept_id
                for v in o.value_as_concept]
        q = q |>
            Where(Fun.or(args = args))
    end
    if o.value_as_number !== nothing
        q = q |>
            Where(predicate(o.value_as_number, ctx, field = Get.value_as_number))
    end
    if !isempty(o.unit)
        args = [Get.unit_concept_id .== u.concept_id
                for u in o.unit]
        q = q |>
            Where(Fun.or(args = args))
    end
    translate(o.base, ctx, base = q)
end

function translate(o::ObservationPeriod, ctx::TranslateContext)
    @assert isempty(o.period_type)
    @assert !o.period_type_exclude
    @assert o.period_start_date === nothing
    @assert o.period_end_date === nothing
    @assert o.age_at_start === nothing
    @assert o.age_at_end === nothing
    @assert o.user_defined_period === nothing
    q = From(ctx.model.observation_period) |>
        Define(:event_id => Get.observation_period_id,
               :start_date => Get.observation_period_start_date,
               :end_date => Get.observation_period_end_date,
               :sort_date => Get.observation_period_start_date,
               :visit_occurrence_id => 0)
    if o.period_length !== nothing
        field = Fun.datediff_day(Get.end_date, Get.start_date)
        q = q |>
            Where(predicate(o.period_length, ctx, field = field))
    end
    translate(o.base, ctx, base = q)
end

function translate(p::ProcedureOccurrence, ctx::TranslateContext)
    @assert isempty(p.procedure_type)
    @assert !p.procedure_type_exclude
    @assert isempty(p.modifier)
    @assert p.quantity === nothing
    q = From(ctx.model.procedure_occurrence) |>
        Define(:concept_id => Get.procedure_concept_id,
               :event_id => Get.procedure_occurrence_id,
               :start_date => Get.procedure_date,
               :end_date => dateadd_day(Get.procedure_date, 1),
               :sort_date => Get.procedure_date)
    if p.procedure_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.procedure_source_concept_id,
                         translate(find_concept_set(p.procedure_source_concept, ctx), ctx)))
    end
    translate(p.base, ctx, base = q)
end

function translate(v::VisitOccurrence, ctx::TranslateContext)
    @assert isempty(v.place_of_service)
    @assert v.place_of_service_location === nothing
    @assert v.visit_length === nothing
    @assert !v.visit_type_exclude
    q = From(ctx.model.visit_occurrence) |>
        Define(:concept_id => Get.visit_concept_id,
               :event_id => Get.visit_occurrence_id,
               :start_date => Get.visit_start_date,
               :end_date => Get.visit_end_date,
               :sort_date => Get.visit_start_date)
    if v.visit_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.visit_source_concept_id,
                         translate(find_concept_set(v.visit_source_concept, ctx), ctx)))
    end
    translate(v.base, ctx, base = q)
end

is_simple(c, ctx::TranslateContext) =
    is_simple(c.base, ctx)

is_simple(b::BaseCriteria, ctx::TranslateContext) =
    b.correlated_criteria === nothing

function translate(b::BaseCriteria, ctx::TranslateContext; base)
    @assert b.occurrence_end_date === nothing
    q = base
    if b.codeset_id !== nothing
        q = q |>
            Where(Fun.in(Get.concept_id,
                         translate(find_concept_set(b.codeset_id, ctx), ctx)))
    end
    #=
    q = Join(:concept => translate(find_concept_set(b.codeset_id, ctx), ctx),
             Get.concept_id .== Get.concept.concept_id)
    =#
    if b.first
        q = q |>
            Partition(Get.person_id, order_by = [Get.sort_date, Get.event_id]) |>
            Where(Agg.row_number() .== 1)
    end
    if b.occurrence_start_date !== nothing
        q = q |>
            Where(predicate(b.occurrence_start_date, ctx, field = Get.start_date))
    end
    if b.age !== nothing || !isempty(b.gender)
        q = q |>
            Join(:person => ctx.model.person,
                 Get.person_id .== Get.person.person_id)
    end
    if b.age !== nothing
        q = q |>
            Define(:age => Fun.extract_year(Get.start_date) .- Get.person.year_of_birth) |>
            Where(predicate(b.age, ctx, field = Get.age))
    end
    if !isempty(b.gender)
        args = [Get.person.gender_concept_id .== c.concept_id
                for c in b.gender]
        q = q |>
            Where(Fun.or(args = args))
    end
    if !isempty(b.provider_specialty)
        args = [Get.provider.specialty_concept_id .== c.concept_id
                for c in b.provider_specialty]
        q = q |>
            Join(:provider => ctx.model.provider,
                 Get.provider_id .== Get.provider.provider_id) |>
            Where(Fun.or(args = args))
    end
    if !isempty(b.visit_type)
        args = [Get.visit.visit_concept_id .== c.concept_id
                for c in b.visit_type]
        q = q |>
            Join(:visit => ctx.model.visit_occurrence,
                 Fun.and(Get.person_id .== Get.visit.person_id,
                         Get.visit_occurrence_id .== Get.visit.visit_occurrence_id)) |>
            Where(Fun.or(args = args))
    end
    if b.correlated_criteria !== nothing
        q = q |>
            Join(:op_ => ctx.model.observation_period,
                 Get.person_id .== Get.op_.person_id)
        q = q |>
            Define(:op_start_date => Get.op_.observation_period_start_date,
                   :op_end_date => Get.op_.observation_period_end_date)
        q = q |>
            Where(Fun.and(Get.op_start_date .<= Get.start_date, Get.start_date .<= Get.op_end_date))
        q = as_temp_table(ctx, q)
        q = q |>
            translate(b.correlated_criteria, ctx, base = q)
    end
    q
end

is_inner(c::CriteriaGroup) =
    c.type == ALL_CRITERIA && all(is_inner, c.correlated_criteria) && all(is_inner, c.groups)

join_subsumes_where(c::CriteriaGroup) =
    c.type == ALL_CRITERIA && all(join_subsumes_where, c.correlated_criteria) && all(join_subsumes_where, c.groups) && isempty(c.demographic_criteria)

is_simple(c::CriteriaGroup, ctx::TranslateContext) =
    isempty(c.groups) && all(cc -> is_simple(cc, ctx), c.correlated_criteria) && ctx.dialect !== :redshift

function translate(c::CriteriaGroup, ctx::TranslateContext; base::SQLNode, result_alias = nothing, inner = is_inner(c))
    !isempty(c) || return Define()
    is_all = c.type == ALL_CRITERIA
    is_any = c.type == ANY_CRITERIA || (c.type == AT_LEAST_CRITERIA && c.count == 1)
    is_none = c.type == AT_MOST_CRITERIA && c.count == 0
    @assert is_all || is_any || is_none
    if !isempty(c.demographic_criteria)
        q = Join(:person => ctx.model.person,
                 Get.person_id .== Get.person.person_id) |>
            Define(:age => Fun.extract_year(Get.start_date) .- Get.person.year_of_birth)
    else
        q = Define()
    end
    if is_simple(c, ctx)
        p = predicate(c, ctx)
        if result_alias !== nothing
            q = q |>
                Define(result_alias => Fun.case(p, 1, 0))
        else
            q = q |>
                  Where(p)
        end
        return q
    end
    idx = 1
    args = SQLNode[]
    for criteria in c.correlated_criteria
        nested_alias = next_group_alias(ctx)
        q = q |>
            translate(criteria, ctx, base = base, result_alias = nested_alias, inner = inner)
        if !(inner && join_subsumes_where(criteria))
            push!(args, Get(nested_alias) .== 1)
        end
    end
    for criteria in c.demographic_criteria
        nested_alias = next_group_alias(ctx)
        q = q |>
            translate(criteria, ctx, result_alias = nested_alias)
        push!(args, Get(nested_alias) .== 1)
    end
    for group in c.groups
        nested_alias = next_group_alias(ctx)
        q = q |>
            translate(group, ctx, base = base, result_alias = nested_alias, inner = inner)
        if !(inner && join_subsumes_where(group))
            push!(args, Get(nested_alias) .== 1)
        end
    end
    if is_all
        p = Fun.and(args = args)
    elseif is_any
        p = Fun.or(args = args)
    elseif is_none
        args = [Fun.not(arg) for arg in args]
        p = Fun.and(args = args)
    end
    if result_alias !== nothing
        q = q |>
            Define(result_alias => Fun.case(p, 1, 0))
    else
        q = q |>
            Where(p)
    end
    q
end

function predicate(c::CriteriaGroup, ctx::TranslateContext)
    is_all = c.type == ALL_CRITERIA
    is_any = c.type == ANY_CRITERIA || (c.type == AT_LEAST_CRITERIA && c.count == 1)
    is_none = c.type == AT_MOST_CRITERIA && c.count == 0
    @assert is_all || is_any || is_none
    args = SQLNode[]
    for criteria in c.correlated_criteria
        push!(args, predicate(criteria, ctx))
    end
    for criteria in c.demographic_criteria
        push!(args, predicate(criteria, ctx))
    end
    for group in c.groups
        push!(args, predicate(group, ctx))
    end
    if is_all
        Fun.and(args = args)
    elseif is_any
        Fun.or(args = args)
    elseif is_none
        args = [Fun.not(arg) for arg in args]
        Fun.and(args = args)
    end
end

function translate(d::DemographicCriteria, ctx::TranslateContext; result_alias)
    Define(result_alias => Fun.case(predicate(d, ctx), 1, 0))
end

function predicate(d::DemographicCriteria, ctx::TranslateContext)
    @assert isempty(d.ethnicity)
    @assert isempty(d.race)
    @assert isempty(d.gender)
    @assert d.occurrence_end_date === nothing
    args = SQLNode[]
    if d.age !== nothing
        push!(args, predicate(d.age, ctx, field = Get.age))
    end
    if d.occurrence_start_date !== nothing
        push!(args, predicate(d.occurrence_start_date, ctx, field = Get.start_date))
    end
    Fun.and(args = args)
end

is_inner(c::CorrelatedCriteria) =
    c.occurrence.type in (AT_LEAST, EXACTLY) && c.occurrence.count > 0

join_subsumes_where(c::CorrelatedCriteria) =
    c.occurrence.type == AT_LEAST && c.occurrence.count == 1

is_simple(c::CorrelatedCriteria, ctx::TranslateContext) =
    (c.occurrence.type == AT_LEAST && c.occurrence.count == 1) &&
    is_simple(c.criteria, ctx)

function translate(c::CorrelatedCriteria, ctx::TranslateContext; base, result_alias, inner)
    @assert c.occurrence !== nothing &&
            c.occurrence.count_column === nothing
    @assert c.occurrence.type in (AT_LEAST, AT_MOST, EXACTLY)
    @assert c.criteria !== nothing
    q = base |>
        Join(:correlated => translate(c.criteria, ctx),
             Get.correlated.person_id .== Get.person_id)
    if c.restrict_visit
        q = q |>
            Where(Get.correlated.visit_occurrence_id .== Get.visit_occurrence_id)
    end
    if !c.ignore_observation_period
        q = q |>
            Where(Fun.and(Get.op_start_date .<= Get.correlated.start_date,
                          Get.correlated.start_date .<= Get.op_end_date))
    end
    q = q |>
        translate(c.start_window, ctx, start = true, ignore_observation_period = c.ignore_observation_period)
    if c.end_window !== nothing
        q = q |>
            translate(c.end_window, ctx, start = false, ignore_observation_period = c.ignore_observation_period)
    end
    q = q |>
        Group(Get.person_id, Get.event_id)
    if c.occurrence.is_distinct
        q = q |>
            Define(:count => Agg.count_distinct(Get.correlated.concept_id))
    else
        q = q |>
            Define(:count => Agg.count())
    end
    correlated_alias = Symbol(:correlated_, result_alias)
    q = Join(correlated_alias => q,
             Fun.and(Get.person_id .== (Get(correlated_alias) |> Get.person_id),
                     Get.event_id .== (Get(correlated_alias) |> Get.event_id)),
             left = !inner)
    count = Fun.coalesce(Get(correlated_alias) |> Get.count, 0)
    if c.occurrence.type == AT_LEAST
        p = count .>= c.occurrence.count
    elseif c.occurrence.type == AT_MOST
        p = count .<= c.occurrence.count
    elseif c.occurrence.type == EXACTLY
        p = count .== c.occurrence.count
    end
    q = q |>
        Define(result_alias => Fun.case(p, 1, 0))
    q
end

function predicate(c::CorrelatedCriteria, ctx::TranslateContext)
    @assert c.occurrence !== nothing &&
            c.occurrence.count_column === nothing
    @assert c.occurrence.type in (AT_LEAST, AT_MOST, EXACTLY)
    @assert c.criteria !== nothing
    q = translate(c.criteria, ctx)
    q = q |>
        As(:correlated) |>
        Where(Get.correlated.person_id .== Var.person_id)
    if c.restrict_visit
        q = q |>
            Where(Get.correlated.visit_occurrence_id .== Var.visit_occurrence_id)
    end
    q = q |>
        Define(:start_date => Var.start_date,
               :end_date => Var.end_date,
               :op_start_date => Var.op_start_date,
               :op_end_date => Var.op_end_date)
    if !c.ignore_observation_period
        q = q |>
            Where(Fun.and(Get.op_start_date .<= Get.correlated.start_date,
                          Get.correlated.start_date .<= Get.op_end_date))
    end
    q = q |>
        translate(c.start_window, ctx, start = true, ignore_observation_period = c.ignore_observation_period)
    if c.end_window !== nothing
        q = q |>
            translate(c.end_window, ctx, start = false, ignore_observation_period = c.ignore_observation_period)
    end
    exists = c.occurrence.type == AT_LEAST && c.occurrence.count == 1
    not_exists = c.occurrence.type in (EXACTLY, AT_MOST) && c.occurrence.count == 0
    if !exists && !not_exists
        q = q |>
            Group()
        if c.occurrence.is_distinct
            q = q |>
                Select(Agg.count_distinct(Get.correlated.concept_id))
        else
            q = q |>
                Select(Agg.count())
        end
    end
    q = q |>
        Bind(:person_id => Get.person_id,
             :start_date => Get.start_date,
             :end_date => Get.end_date,
             :op_start_date => Get.op_start_date,
             :op_end_date => Get.op_end_date,
             :visit_occurrence_id => c.restrict_visit ? Get.visit_occurrence_id : 0)
    if exists
        q = Fun.exists(q)
    elseif not_exists
        q = Fun.not(Fun.exists(q))
    else
        if c.occurrence.type == AT_LEAST
            q = q .>= c.occurrence.count
        elseif c.occurrence.type == AT_MOST
            q = q .<= c.occurrence.count
        elseif c.occurrence.type == EXACTLY
            q = q .== c.occurrence.count
        end
    end
    q
end

function translate(w::Window, ctx::TranslateContext; start::Bool, ignore_observation_period::Bool)
    index_date_field =
        w.use_index_end == true ? Get.end_date : Get.start_date
    event_date_field =
        if start
            w.use_event_end == true ?
                Get.correlated.end_date : Get.correlated.start_date
        else
            w.use_event_end == false ?
                Get.correlated.start_date : Get.correlated.end_date
        end
    l = nothing
    r = nothing
    if w.start.days !== nothing
        l = dateadd_day(index_date_field, w.start.days * w.start.coeff)
    elseif !ignore_observation_period
        l = w.start.coeff == -1 ? Get.op_start_date : Get.op_end_date
    end
    if w.end_.days !== nothing
        r = dateadd_day(index_date_field, w.end_.days * w.end_.coeff)
    elseif !ignore_observation_period
        r = w.end_.coeff == -1 ? Get.op_start_date : Get.op_end_date
    end
    args = []
    if l !== nothing
        push!(args, l .<= event_date_field)
    end
    if r !== nothing
        push!(args, event_date_field .<= r)
    end
    Where(Fun.and(args = args))
end

function translate(c::CollapseSettings, ctx::TranslateContext)
    @assert c.collapse_type == ERA
    gap = c.era_pad
    Define(:end_date => dateadd_day(Get.end_date, gap)) |>
    Partition(Get.person_id, order_by = [Get.start_date], frame = (mode = :rows, start = -Inf, finish = -1)) |>
    Define(:boundary => Agg.max(Get.end_date)) |>
    Define(:bump => Fun.case(Get.start_date .<= Get.boundary, 0, 1)) |>
    Partition(Get.person_id, order_by = [Get.start_date, .- Get.bump], frame = :rows) |>
    Define(:group => Agg.sum(Get.bump)) |>
    Group(Get.person_id, Get.group) |>
    Define(:start_date => Agg.min(Get.start_date),
           :end_date => dateadd_day(Agg.max(Get.end_date), - gap))
end

function find_concept_set(codeset_id, ctx)
    for cs in ctx.cohort.concept_sets
        if cs.id == codeset_id
            return cs
        end
    end
end

function translate(c::ConceptSet, ctx::TranslateContext)
    include = ConceptSetItem[]
    exclude = ConceptSetItem[]
    for item in c.items
        @assert !item.include_mapped
        if !item.is_excluded
            push!(include, item)
        else
            push!(exclude, item)
        end
    end
    q = translate(include, ctx)
    if !isempty(exclude)
        q = q |>
            LeftJoin(:excluded => translate(exclude, ctx),
                     Get.concept_id .== Get.excluded.concept_id) |>
            Where(Fun."is null"(Get.excluded.concept_id))
    end
    q = q |>
        Select(Get.concept_id)
    q
end

function translate(items::Vector{ConceptSetItem}, ctx::TranslateContext)
    # TODO: Fun.in
    args = [Get.concept_id .== item.concept.concept_id for item in items]
    q = From(ctx.model.concept) |>
        Where(Fun.or(args = args))
    with_descendants = [item for item in items if item.include_descendants]
    if !isempty(with_descendants)
        args = [Get.ancestor_concept_id .== item.concept.concept_id for item in with_descendants]
        q = q |>
            Append(
                From(ctx.model.concept) |>
                Where(Fun."is null"(Get.invalid_reason)) |>
                Join(ctx.model.concept_ancestor,
                     Get.concept_id .== Get.descendant_concept_id) |>
                Where(Fun.or(args = args))) |>
            Group(Get.concept_id)
    end
    q
end

function predicate(r::Union{NumericRange, DateRange}, ctx::TranslateContext; field)
    if r.op == GT
        field .> r.value
    elseif r.op == GTE
        field .>= r.value
    elseif r.op == LT
        field .< r.value
    elseif r.op == LTE
        field .<= r.value
    elseif r.op == EQ
        field .== r.value
    elseif r.op == NEQ
        field .!= r.value
    elseif r.op == BT
        Fun.between(field, r.value, r.extent)
    elseif r.op == NBT
        Fun."not between"(field, r.value, r.extent)
    end
end

