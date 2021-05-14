using JSON
using PrettyPrinting
using FunSQL:
    FunSQL, Define, From, FUN, OP, Fun, FunctionNode, Get, Join, LeftJoin,
    Select, Where, Partition, Agg, Group, As

import ..Model, ..Source

function FunSQL.translate(::Val{:dateadd_day}, n::FunctionNode, treq)
    args = FunSQL.translate(n.args, treq)
    if length(args) == 2
        if treq.ctx.dialect.name === :sqlserver
            return FUN(:DATEADD, args = [OP(:day), args[2], args[1]])
        else
            return OP(:+, args = [args[1], args[2]])
        end
    end
    FunSQL.translate_default(n, treq)
end

translate(cohort; dialect, model = Model()) =
    translate(cohort, dialect, model)

translate(cohort, source::Source) =
    translate(cohort, source.dialect, source.model)

translate(cohort::String, dialect, model) =
    translate(JSON.parse(cohort), dialect, model)

translate(cohort::Dict, dialect, model) =
    translate(unpack!(cohort), dialect, model)

struct TranslateContext
    dialect::Symbol
    model::Model
    cohort::CohortExpression
end

translate(cohort::CohortExpression, dialect, model) =
    translate(cohort, TranslateContext(dialect, model, cohort))

function translate(c::CohortExpression, ctx::TranslateContext)
    pprintln(c)
    @assert c.censor_window.start_date === c.censor_window.end_date === nothing
    @assert isempty(c.censoring_criteria)
    @assert c.end_strategy isa DateOffsetStrategy
    @assert c.expression_limit.type == ALL
    @assert isempty(c.inclusion_rules)
    @assert c.qualified_limit.type == ALL
    q = translate(c.primary_criteria, ctx)
    q = q |>
        translate(c.end_strategy, ctx)
    q = q |>
        translate(c.additional_criteria, ctx)
    q = q |>
        translate(c.collapse_settings, ctx)
    q = q |>
        Select(Get.person_id, Get.start_date, Get.end_date)
    q
end

translate(::Nothing, ctx::TranslateContext) =
    Define()

function translate(d::DateOffsetStrategy, ctx::TranslateContext)
    field =
        d.date_field == START_DATE ? Get.start_date :
        d.date_field == END_DATE ? Get.end_date :
        nothing
    Define(:end_date => dateadd_day(field, d.offset))
end

function dateadd_day(n, delta::Integer)
    if iszero(delta)
        return n
    end
    Fun.dateadd_day(n, delta)
end

function translate(c::PrimaryCriteria, ctx::TranslateContext)
    @assert length(c.criteria_list) == 1
    @assert c.primary_limit.type == ALL
    q = translate(c.criteria_list[1], ctx)
    q = q |>
        Join(:op => ctx.model.observation_period |>
                    Define(:start_date => Get.observation_period_start_date,
                           :end_date => Get.observation_period_end_date),
             Get.person_id .== Get.op.person_id)
    l = dateadd_day(Get.op.start_date, c.observation_window.prior_days)
    r = dateadd_day(Get.op.end_date, c.observation_window.post_days)
    q = q |>
        Where(Fun.and(l .<= Get.start_date, Get.start_date .<= r))
    q
end

function translate(c::ConditionOccurrence, ctx::TranslateContext)
    @assert c.condition_source_concept === nothing
    @assert isempty(c.condition_status)
    @assert isempty(c.condition_type)
    @assert !c.condition_type_exclude
    @assert c.stop_reason === nothing
    q = From(ctx.model.condition_occurrence) |>
        Define(:concept_id => Get.condition_concept_id,
               :start_date => Get.condition_start_date,
               :end_date => Fun.coalesce(Get.condition_end_date,
                                         dateadd_day(Get.condition_start_date, 1)))
    q = q |>
        translate(c.base, ctx)
    q
end

function translate(v::VisitOccurrence, ctx::TranslateContext)
    @assert isempty(v.place_of_service)
    @assert v.place_of_service_location === nothing
    @assert v.visit_source_concept === nothing
    @assert v.visit_length === nothing
    @assert !v.visit_type_exclude
    q = From(ctx.model.visit_occurrence) |>
        Define(:concept_id => Get.visit_concept_id,
               :start_date => Get.visit_start_date,
               :end_date => Get.visit_end_date)
    q = q |>
        translate(v.base, ctx)
    q
end

function translate(b::BaseCriteria, ctx::TranslateContext)
    @assert b.age === nothing
    @assert b.correlated_criteria === nothing
    @assert !b.first
    @assert isempty(b.gender)
    @assert b.occurrence_end_date === nothing
    @assert b.occurrence_start_date === nothing
    @assert isempty(b.provider_specialty)
    @assert isempty(b.visit_type)
    Where(Fun.in(Get.concept_id,
                 translate(find_concept_set(b.codeset_id, ctx), ctx)))
end

function translate(c::CriteriaGroup, ctx::TranslateContext)
    @assert c.count === nothing
    @assert length(c.correlated_criteria) == 1
    @assert isempty(c.demographic_criteria)
    @assert isempty(c.groups)
    @assert c.type == "ALL"
    translate(c.correlated_criteria[1], ctx)
end

function translate(c::CorrelatedCriteria, ctx::TranslateContext)
    @assert !c.ignore_observation_period
    @assert !c.restrict_visit
    @assert c.occurrence !== nothing &&
            c.occurrence.type == AT_LEAST &&
            c.occurrence.count == 1 &&
            !c.occurrence.is_distinct &&
            c.occurrence.count_column === nothing
    @assert c.criteria !== nothing
    q = translate(c.criteria, ctx)
    q = Join(q |> As(:correlated),
             Get.person_id .== Get.correlated.person_id)
    q = q |>
        Where(Fun.and(Get.op.start_date .<= Get.correlated.start_date,
                      Get.correlated.start_date .<= Get.op.end_date))
    q = q |>
        translate(c.start_window, true, ctx)
    if c.end_window !== nothing
        q = q |>
        translate(c.end_window, false, ctx)
    end
end

function translate(w::Window, start::Bool, ctx::TranslateContext)
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
    elseif w.start.coeff == -1
        l = Get.op.start_date
    else
        l = Get.op.end_date
    end
    if w.end_.days !== nothing
        r = dateadd_day(index_date_field, w.end_.days * w.end_.coeff)
    elseif w.end_.coeff == -1
        r = Get.op.start_date
    else
        r = Get.op.end_date
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
    Partition(Get.person_id, order_by = [Get.start_date]) |>
    Define(:boundary => Agg.lag(Get.end_date)) |>
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
    include = Int[]
    exclude = Int[]
    for item in c.items
        @assert item.include_descendants
        @assert !item.include_mapped
        if !item.is_excluded
            push!(include, item.concept.concept_id)
        else
            push!(exclude, item.concept.concept_id)
        end
    end
    q = translate_concept(include, ctx)
    if !isempty(exclude)
        q = q |>
            LeftJoin(:excluded => translate_concept(exclude, ctx),
                     Get.concept_id .== Get.excluded.concept_id) |>
            Where(Fun."is null"(Get.excluded.concept_id))
    end
    q = q |>
        Select(Get.concept_id)
    q
end

function translate_concept(ids::Vector{Int}, ctx::TranslateContext)
    # TODO: Fun.in
    q = From(ctx.model.concept) |>
        Where(Fun."is null"(Get.invalid_reason)) |>
        Join(ctx.model.concept_ancestor,
             Get.concept_id .== Get.descendant_concept_id) |>
        Where(Fun.or(args = [Get.ancestor_concept_id .== id for id in ids]))
    q
end

