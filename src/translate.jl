using JSON
using Dates
using PrettyPrinting
using FunSQL:
    FunSQL, Agg, Append, As, Bind, Define, From, Fun, Get, Group, Join, LeftJoin,
    Partition, Select, Var, Where, With, render, SQLClause, SQLNode, SQLTable, ID

struct TranslateContext
end

struct SwitchByDialectNode <: FunSQL.AbstractSQLNode
    over::Union{SQLNode, Nothing}
    cases::Vector{Symbol}
    branches::Vector{SQLNode}
    default::SQLNode

    SwitchByDialectNode(; over = nothing, cases, branches, default) =
        new(over, cases, branches, default)
end

SwitchByDialect(args...; kws...) =
    SwitchByDialectNode(args...; kws...) |> SQLNode

function FunSQL.quoteof(n::SwitchByDialectNode, ctx)
    ex = Expr(:call, nameof(SwitchByDialect))
    push!(ex.args, Expr(:kw, :cases, Expr(:vect, Any[QuoteNode(case) for case in n.cases]...)))
    push!(ex.args, Expr(:kw, :branches, Expr(:vect, Any[FunSQL.quoteof(branch, ctx) for branch in n.branches]...)))
    push!(ex.args, Expr(:kw, :default, FunSQL.quoteof(n.default, ctx)))
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::SwitchByDialectNode, ctx)
    q = n.default
    for (i, case) in enumerate(n.cases)
        if case === ctx.catalog.dialect.name
            q = n.branches[i]
            break
        end
    end
    over = n.over
    if over !== nothing
        q = over |> q
    end
    FunSQL.resolve(q, ctx)
end

function FunSQL.resolve_scalar(n::SwitchByDialectNode, ctx)
    q = n.default
    for (i, case) in enumerate(n.cases)
        if case === ctx.catalog.dialect.name
            q = n.branches[i]
            break
        end
    end
    over = n.over
    if over !== nothing
        q = over |> q
    end
    FunSQL.resolve_scalar(q, ctx)
end

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
    if ctx.dialect.name === :sqlserver || ctx.dialect.name === :spark
        FunSQL.@serialize! "datediff(day, ?, ?)" [args[2], args[1]] ctx
    else
        FunSQL.@serialize! "-" args ctx
    end
end

translate(c::AbstractString; cohort_definition_id = 0) =
    translate(JSON.parse(c), cohort_definition_id = cohort_definition_id)

translate(c::Dict; cohort_definition_id = 0) =
    translate(unpack!(deepcopy(c)), cohort_definition_id = cohort_definition_id)

function translate(c::CohortExpression; cohort_definition_id = 0)
    @assert c.censor_window.start_date === c.censor_window.end_date === nothing
    q = translate(c.primary_criteria)
    if c.additional_criteria !== nothing && !isempty(c.additional_criteria)
        q = q |>
            translate(c.additional_criteria)
        q = q |>
            translate(c.qualified_limit)
    end
    for r in c.inclusion_rules
        q = q |>
            translate(r.expression)
    end
    q = q |>
        translate(c.expression_limit)
    if c.end_strategy !== nothing
        q = q |>
            translate(c.end_strategy)
    else
        q = q |>
            Define(:end_date => Get.op_end_date)
    end
    q = q |>
        Partition(order_by = [Get.person_id, Get.event_id]) |>
        Define(:row_number => Agg.row_number())
    for cc in c.censoring_criteria
        q = q |>
            LeftJoin(:censoring => translate(cc),
                     Fun.and(Get.person_id .== Get.censoring.person_id,
                             Get.start_date .<= Get.censoring.start_date,
                             Get.op_end_date .>= Get.censoring.start_date)) |>
            Partition(Get.row_number, order_by = [Get.row_number]) |>
            Where(Agg.row_number() .== 1) |>
            Define(:end_date => Fun.least(Get.end_date, Agg.min(Get.censoring.start_date)))
    end
    q = q |>
        translate(c.collapse_settings)
    for s in c.concept_sets
        q = q |>
            translate(s)
    end
    q = q |>
        Select(
            :cohort_definition_id => cohort_definition_id,
            :subject_id => Get.person_id,
            :cohort_start_date => Get.start_date,
            :cohort_end_date => Get.end_date)
end

function translate(r::ResultLimit; order_by = [Get.start_date])
    if r.type == ALL
        return Define()
    end
    if r.type == LAST
        order_by = [Fun.datediff_day(order_by[1], Date(2020, 1, 1)), order_by[2:end]...]
    end
    Partition(Get.person_id, order_by = order_by) |>
    Where(Agg.row_number() .== 1)
end

function translate(d::DateOffsetStrategy)
    field =
        d.date_field == START_DATE ? Get.start_date :
        d.date_field == END_DATE ? Get.end_date :
        nothing
    Define(:end_date => dateadd_day(field, d.offset)) |>
    Define(:end_date => Fun.case(Get.end_date .<= Get.op_end_date,
                                 Get.end_date, Get.op_end_date))
end

function translate(s::CustomEraStrategy)
    @assert s.offset == 0
    @assert s.days_supply_override === nothing
    gap = s.gap_days
    q = From(:drug_exposure) |>
        Where(Fun.or(Fun.in(Get.drug_concept_id, From("concept_set_$(s.drug_codeset_id)") |> Select(Get.concept_id)),
                     Fun.in(Get.drug_source_concept_id, From("concept_set_$(s.drug_codeset_id)") |> Select(Get.concept_id)))) |>
        Define(:start_date => Get.drug_exposure_start_date,
               :end_date => Fun.coalesce(Get.drug_exposure_end_date,
                                         Fun.dateadd_day(Get.drug_exposure_start_date, Get.days_supply),
                                         dateadd_day(Get.drug_exposure_start_date, 1))) |>
        Define(:end_date => dateadd_day(Get.end_date, gap)) |>
        Partition(Get.person_id, order_by = [Get.start_date], frame = (mode = :rows, start = -Inf, finish = -1)) |>
        Define(:boundary => Agg.max(Get.end_date)) |>
        Define(:bump => Fun.case(Get.start_date .<= Get.boundary, 0, 1)) |>
        Partition(Get.person_id, order_by = [Get.start_date, .- Get.bump], frame = :rows) |>
        Define(:group => Agg.sum(Get.bump)) |>
        Group(Get.person_id, Get.group) |>
        Define(:start_date => Agg.min(Get.start_date),
               :end_date => dateadd_day(Agg.max(Get.end_date), - gap))
    q = LeftJoin(:custom_era => q,
                 Fun.and(Get.person_id .== Get.custom_era.person_id,
                         Fun.between(Get.start_date, Get.custom_era.start_date, Get.custom_era.end_date))) |>
        Define(:end_date => Fun.least(Get.op_end_date, Get.custom_era.end_date))
    q
end

function dateadd_day(n, delta::Integer)
    if iszero(delta)
        return n
    end
    Fun.dateadd_day(n, delta)
end

function translate(c::PrimaryCriteria)
    @assert length(c.criteria_list) >= 1
    q = translate(c.criteria_list[1])
    if length(c.criteria_list) > 1
        args = [translate(l) for l in c.criteria_list[2:end]]
        q = q |>
            Append(args = args)
    end
    q = q |>
        Join(:op => From(:observation_period) |>
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
        translate(c.primary_limit, order_by = [Get.sort_date, Get.event_id])
    q
end

function translate(d::ConditionEra)
    @assert d.era_start_date === nothing
    @assert d.era_end_date === nothing
    @assert d.era_length === nothing
    @assert d.age_at_start === nothing
    @assert d.age_at_end === nothing
    q = From(:condition_era) |>
        Define(:concept_id => Get.condition_concept_id,
               :event_id => Get.condition_era_id,
               :start_date => Get.condition_era_start_date,
               :end_date => Get.condition_era_end_date,
               :sort_date => Get.condition_era_start_date,
               :visit_occurrence_id => 0)
    if d.era_length !== nothing
        field = Fun.datediff_day(Get.drug_era_end_date, Get.drug_era_start_date)
        q = q |>
            Where(translate(d.era_length) |> Bind(:field => field))
    end
    if d.occurrence_count !== nothing
        q = q |>
            Where(translate(d.occurrence_count) |> Bind(:field => Get.condition_occurrence_count))
    end
    q = q |>
        translate(d.base)
    q
end

function translate(c::ConditionOccurrence)
    @assert isempty(c.condition_status)
    @assert c.stop_reason === nothing
    q = From(:condition_occurrence) |>
        Define(:concept_id => Get.condition_concept_id,
               :event_id => Get.condition_occurrence_id,
               :start_date => Get.condition_start_date,
               :end_date => Fun.coalesce(Get.condition_end_date,
                                         dateadd_day(Get.condition_start_date, 1)),
               :sort_date => Get.condition_start_date)
    if c.condition_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.condition_source_concept_id,
                         From("concept_set_$(c.condition_source_concept)") |> Select(Get.concept_id)))
    end
    if !isempty(c.condition_type)
        args = SQLNode[Get.condition_type_concept_id, SQLNode[t.concept_id for t in c.condition_type]...]
        if !c.condition_type_exclude
            p = Fun.in(args = args)
        else
            p = Fun."not in"(args = args)
        end
        q = q |>
            Where(p)
    end
    q = q |>
        translate(c.base)
    q
end

function translate(d::Death)
    @assert isempty(d.death_type)
    @assert !d.death_type_exclude
    q = From(:death) |>
        Define(:concept_id => Get.cause_concept_id,
               :event_id => 0,
               :start_date => Get.death_date,
               :end_date => dateadd_day(Get.death_date, 1),
               :sort_date => Get.death_date,
               :visit_occurrence_id => 0)
    if d.death_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.cause_source_concept_id,
                         From("concept_set_$(d.death_source_concept)") |> Select(Get.concept_id)))
    end
    q = q |>
        translate(d.base)
    q
end

function translate(d::DeviceExposure)
    @assert isempty(d.device_type)
    @assert !d.device_type_exclude
    @assert d.quantity === nothing
    @assert d.unique_device_id === nothing
    q = From(:device_exposure) |>
        Define(:concept_id => Get.device_concept_id,
               :event_id => Get.device_exposure_id,
               :start_date => Get.device_exposure_start_date,
               :end_date => Fun.coalesce(Get.device_exposure_end_date,
                                         dateadd_day(Get.device_exposure_start_date, 1)),
               :sort_date => Get.device_exposure_start_date)
    if d.device_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.device_source_concept_id,
                         From("concept_set_$(d.device_source_concept)") |> Select(Get.concept_id)))
    end
    q = q |>
        translate(d.base)
    q
end

function translate(d::DoseEra)
    @assert d.dose_value === nothing
    @assert d.era_start_date === nothing
    @assert d.era_end_date === nothing
    @assert d.age_at_start === nothing
    @assert d.age_at_end === nothing
    @assert isempty(d.unit)
    q = From(:dose_era) |>
        Define(:concept_id => Get.drug_concept_id,
               :event_id => Get.dose_era_id,
               :start_date => Get.dose_era_start_date,
               :end_date => Get.dose_era_end_date,
               :sort_date => Get.dose_era_start_date,
               :visit_occurrence_id => 0)
    if d.era_length !== nothing
        field = Fun.datediff_day(Get.dose_era_end_date, Get.dose_era_start_date)
        q = q |>
            Where(translate(d.era_length) |> Bind(:field => field))
    end
    q = q |>
        translate(d.base)
    q
end

function translate(d::DrugEra)
    @assert d.era_start_date === nothing
    @assert d.era_end_date === nothing
    @assert d.occurrence_count === nothing
    @assert d.age_at_start === nothing
    @assert d.age_at_end === nothing
    q = From(:drug_era) |>
        Define(:concept_id => Get.drug_concept_id,
               :event_id => Get.drug_era_id,
               :start_date => Get.drug_era_start_date,
               :end_date => Get.drug_era_end_date,
               :sort_date => Get.drug_era_start_date,
               :visit_occurrence_id => 0)
    if d.era_length !== nothing
        field = Fun.datediff_day(Get.drug_era_end_date, Get.drug_era_start_date)
        q = q |>
            Where(translate(d.era_length) |> Bind(:field => field))
    end
    q = q |>
        translate(d.base)
    q
end

function translate(d::DrugExposure)
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
    q = From(:drug_exposure) |>
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
                         From("concept_set_$(d.drug_source_concept)") |> Select(Get.concept_id)))
    end
    q = q |>
        translate(d.base)
    q
end

function translate(m::Measurement)
    @assert isempty(m.measurement_type)
    @assert !m.measurement_type_exclude
    @assert m.abnormal === nothing
    @assert isempty(m.operator)
    q = From(:measurement) |>
        Define(:concept_id => Get.measurement_concept_id,
               :event_id => Get.measurement_id,
               :start_date => Get.measurement_date,
               :end_date => dateadd_day(Get.measurement_date, 1),
               :sort_date => Get.measurement_date)
    if m.measurement_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.measurement_source_concept_id,
                         From("concept_set_$(m.measurement_source_concept)") |> Select(Get.concept_id)))
    end
    if !isempty(m.value_as_concept)
        args = [Get.value_as_concept_id .== v.concept_id
                for v in m.value_as_concept]
        q = q |>
            Where(Fun.or(args = args))
    end
    if m.range_low !== nothing
        q = q |>
            Where(translate(m.range_low) |> Bind(:field => Get.range_low))
    end
    if m.range_high !== nothing
        q = q |>
            Where(translate(m.range_high) |> Bind(:field => Get.range_high))
    end
    if m.range_low_ratio !== nothing
        q = q |>
            Where(translate(m.range_low_ratio) |> Bind(:field => Get.value_as_number ./ Fun.nullif(Get.range_low, 0)))
    end
    if m.range_high_ratio !== nothing
        q = q |>
            Where(translate(m.range_high_ratio) |> Bind(:field => Get.value_as_number ./ Fun.nullif(Get.range_high, 0)))
    end
    if m.value_as_number !== nothing
        q = q |>
            Where(translate(m.value_as_number) |> Bind(:field => Get.value_as_number))
    end
    if !isempty(m.unit)
        args = [Get.unit_concept_id .== u.concept_id
                for u in m.unit]
        q = q |>
            Where(Fun.or(args = args))
    end
    q = q |>
        translate(m.base)
    q
end

function translate(o::Observation)
    @assert isempty(o.observation_type)
    @assert !o.observation_type_exclude
    @assert o.value_as_string === nothing
    @assert isempty(o.qualifier)
    q = From(:observation) |>
        Define(:concept_id => Get.observation_concept_id,
               :event_id => Get.observation_id,
               :start_date => Get.observation_date,
               :end_date => dateadd_day(Get.observation_date, 1),
               :sort_date => Get.observation_date)
    if o.observation_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.observation_source_concept_id,
                         From("concept_set_$(o.observation_source_concept)") |> Select(Get.concept_id)))
    end
    if !isempty(o.value_as_concept)
        args = [Get.value_as_concept_id .== v.concept_id
                for v in o.value_as_concept]
        q = q |>
            Where(Fun.or(args = args))
    end
    if o.value_as_number !== nothing
        q = q |>
            Where(translate(o.value_as_number) |> Bind(:field => Get.value_as_number))
    end
    if !isempty(o.unit)
        args = [Get.unit_concept_id .== u.concept_id
                for u in o.unit]
        q = q |>
            Where(Fun.or(args = args))
    end
    q = q |>
        translate(o.base)
    q
end

function translate(o::ObservationPeriod)
    @assert isempty(o.period_type)
    @assert !o.period_type_exclude
    @assert o.period_start_date === nothing
    @assert o.period_end_date === nothing
    @assert o.age_at_start === nothing
    @assert o.age_at_end === nothing
    q = From(:observation_period) |>
        Define(:event_id => Get.observation_period_id,
               :start_date => Get.observation_period_start_date,
               :end_date => Get.observation_period_end_date,
               :sort_date => Get.observation_period_start_date,
               :visit_occurrence_id => 0)
    if o.period_length !== nothing
        field = Fun.datediff_day(Get.end_date, Get.start_date)
        q = q |>
            Where(translate(o.period_length) |> Bind(:field => field))
    end
    if o.user_defined_period !== nothing
        user_start_date = o.user_defined_period.start_date
        user_end_date = o.user_defined_period.end_date
        if user_start_date !== nothing
            q = q |>
                Where(Fun.and(Get.start_date .<= user_start_date,
                              Get.end_date .>= user_start_date))
        end
        if user_end_date !== nothing
            q = q |>
                Where(Fun.and(Get.start_date .<= user_end_date,
                              Get.end_date .>= user_end_date))
        end
        if user_start_date !== nothing
            q = q |>
                Define(:start_date => Fun.cast(user_start_date, "DATE"))
        end
        if user_end_date !== nothing
            q = q |>
                Define(:end_date => Fun.cast(user_end_date, "DATE"))
        end
    end
    q = q |>
        translate(o.base)
    q
end

function translate(p::ProcedureOccurrence)
    @assert isempty(p.procedure_type)
    @assert !p.procedure_type_exclude
    @assert isempty(p.modifier)
    @assert p.quantity === nothing
    q = From(:procedure_occurrence) |>
        Define(:concept_id => Get.procedure_concept_id,
               :event_id => Get.procedure_occurrence_id,
               :start_date => Get.procedure_date,
               :end_date => dateadd_day(Get.procedure_date, 1),
               :sort_date => Get.procedure_date)
    if p.procedure_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.procedure_source_concept_id,
                         From("concept_set_$(p.procedure_source_concept)") |> Select(Get.concept_id)))
    end
    q = q |>
        translate(p.base)
    q
end

function translate(s::Specimen)
    @assert s.specimen_source_concept === nothing
    @assert isempty(s.specimen_type)
    @assert !s.specimen_type_exclude
    @assert s.quantity === nothing
    @assert isempty(s.unit)
    @assert isempty(s.anatomic_site)
    @assert isempty(s.disease_status)
    @assert s.source_id === nothing
    q = From(:specimen) |>
        Define(:concept_id => Get.specimen_concept_id,
               :event_id => Get.specimen_id,
               :start_date => Get.specimen_date,
               :end_date => dateadd_day(Get.specimen_date, 1),
               :sort_date => Get.specimen_date)
    q = q |>
        translate(s.base)
    q
end

function translate(v::VisitDetail)
    @assert v.visit_detail_start_date === nothing
    @assert v.visit_detail_end_date === nothing
    @assert v.visit_detail_type_selection === nothing
    @assert v.visit_detail_length === nothing
    @assert v.gender_selection === nothing
    @assert v.provider_specialty_selection === nothing
    @assert v.place_of_service_selection === nothing
    @assert v.place_of_service_location === nothing
    q = From(:visit_detail) |>
        Define(:concept_id => Get.visit_detail_concept_id,
               :event_id => Get.visit_detail_id,
               :start_date => Get.visit_detail_start_date,
               :end_date => Get.visit_detail_end_date,
               :sort_date => Get.visit_detail_start_date)
   if v.visit_detail_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.visit_detail_source_concept_id,
                         From("concept_set_$(v.visit_detail_source_concept)") |> Select(Get.concept_id)))
    end
     q = q |>
        translate(v.base)
    q
end

function translate(v::VisitOccurrence)
    @assert isempty(v.place_of_service)
    @assert v.place_of_service_location === nothing
    @assert v.visit_length === nothing
    @assert !v.visit_type_exclude
    q = From(:visit_occurrence) |>
        Define(:concept_id => Get.visit_concept_id,
               :event_id => Get.visit_occurrence_id,
               :start_date => Get.visit_start_date,
               :end_date => Get.visit_end_date,
               :sort_date => Get.visit_start_date)
    if v.visit_source_concept !== nothing
        q = q |>
            Where(Fun.in(Get.visit_source_concept_id,
                         From("concept_set_$(v.visit_source_concept)") |> Select(Get.concept_id)))
    end
    q = q |>
        translate(v.base)
    q
end

function translate(b::BaseCriteria)
    @assert b.occurrence_end_date === nothing
    q = Define()
    if b.codeset_id !== nothing
        q = q |>
            Where(Fun.in(Get.concept_id,
                         From("concept_set_$(b.codeset_id)") |> Select(Get.concept_id)))
    end
    if b.first
        q = q |>
            Partition(Get.person_id, order_by = [Get.sort_date, Get.event_id]) |>
            Where(Agg.row_number() .== 1)
    end
    if b.occurrence_start_date !== nothing
        q = q |>
            Where(translate(b.occurrence_start_date) |> Bind(:field => Get.start_date))
    end
    if b.age !== nothing || !isempty(b.gender)
        q = q |>
            Join(:person => From(:person),
                 Get.person_id .== Get.person.person_id)
    end
    if b.age !== nothing
        q = q |>
            Define(:age => Fun.extract_year(Get.start_date) .- Get.person.year_of_birth) |>
            Where(translate(b.age) |> Bind(:field => Get.age))
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
            Join(:provider => From(:provider),
                 Get.provider_id .== Get.provider.provider_id) |>
            Where(Fun.or(args = args))
    end
    if !isempty(b.visit_type)
        args = [Get.visit.visit_concept_id .== c.concept_id
                for c in b.visit_type]
        q = q |>
            Join(:visit => From(:visit_occurrence),
                 Fun.and(Get.person_id .== Get.visit.person_id,
                         Get.visit_occurrence_id .== Get.visit.visit_occurrence_id)) |>
            Where(Fun.or(args = args))
    end
    if b.correlated_criteria !== nothing
        q = q |>
            Join(:op_ => From(:observation_period),
                 Get.person_id .== Get.op_.person_id)
        q = q |>
            Define(:op_start_date => Get.op_.observation_period_start_date,
                   :op_end_date => Get.op_.observation_period_end_date)
        q = q |>
            Where(Fun.and(Get.op_start_date .<= Get.start_date, Get.start_date .<= Get.op_end_date))
        q = q |>
            translate(b.correlated_criteria)
    end
    q
end

function criteria_name!(args, name)
    s = "c$(length(args) + 1)"
    if name !== nothing
        s = "$(name)_$(s)"
    end
    criteria_name = Symbol(s)
    push!(args, Get(criteria_name))
    criteria_name
end

function translate(c::CriteriaGroup; name = nothing)
    !isempty(c) || return Define()
    is_all = c.type == ALL_CRITERIA || (c.type == AT_LEAST_CRITERIA && c.count == length(c.demographic_criteria) + length(c.correlated_criteria) + length(c.groups))
    is_any = c.type == ANY_CRITERIA || (c.type == AT_LEAST_CRITERIA && c.count == 1)
    is_none = c.type == AT_MOST_CRITERIA && c.count == 0
    args = SQLNode[]
    q = Join(:person => From(:person),
             Get.person_id .== Get.person.person_id,
             optional = true) |>
        Define(:age => Fun.extract_year(Get.start_date) .- Get.person.year_of_birth)
    for criteria in c.demographic_criteria
        criteria_name = nothing
        if !(name === nothing && is_all)
            criteria_name = criteria_name!(args, name)
        end
        q = q |>
            translate(criteria, name = criteria_name)
    end
    q = q |>
        Partition(order_by = [Get.person_id, Get.event_id]) |>
        Define(:row_number => Agg.row_number())
    for criteria in c.correlated_criteria
        criteria_name = nothing
        if !(name === nothing && is_all)
            criteria_name = criteria_name!(args, name)
        end
        q = q |>
            translate(criteria, name = criteria_name)
    end
    for group in c.groups
        criteria_name = nothing
        if !(name === nothing && is_all)
            criteria_name = criteria_name!(args, name)
        end
        q = q |>
            translate(group, name = criteria_name)
    end
    if !(name === nothing && is_all)
        if is_all
            p = Fun.and(args = args)
        elseif is_any
            p = Fun.or(args = args)
        elseif is_none
            args = [Fun.not(arg) for arg in args]
            p = Fun.and(args = args)
        else
            args = [Fun.case(arg, 1, 0) for arg in args]
            n = length(args) > 1 ? Fun."+"(args = args) : args[1]
            @assert c.type in (AT_MOST_CRITERIA, AT_LEAST_CRITERIA)
            if c.type == AT_MOST_CRITERIA
                p = n .<= c.count
            elseif c.type == AT_LEAST_CRITERIA
                p = n .>= c.count
            end
        end
        if name !== nothing
            q = q |>
                Define(name => p)
        else
            q = q |>
                Where(p)
        end
    end
    q
end

function translate(d::DemographicCriteria; name = nothing)
    @assert isempty(d.ethnicity)
    @assert isempty(d.race)
    @assert d.occurrence_end_date === nothing
    args = SQLNode[]
    if d.age !== nothing
        push!(args, translate(d.age) |> Bind(:field => Get.age))
    end
    if !isempty(d.gender)
        push!(args, Fun.in(args = SQLNode[Get.person.gender_concept_id, SQLNode[item.concept_id for item in d.gender]...]))
    end
    if d.occurrence_start_date !== nothing
        push!(args, translate(d.occurrence_start_date) |> Bind(:field => Get.start_date))
    end
    p = Fun.and(args = args)
    if name !== nothing
        q = Define(name => p)
    else
        q = Where(p)
    end
    q
end

function translate(c::CorrelatedCriteria; name = nothing)
    @assert c.occurrence !== nothing &&
            (c.occurrence.count_column === nothing || c.occurrence.count_column in ("DOMAIN_CONCEPT", "START_DATE"))
    @assert c.occurrence.type in (AT_LEAST, AT_MOST, EXACTLY)
    @assert c.criteria !== nothing
    on_args = [Get.correlated.person_id .== Get.person_id]
    if c.restrict_visit
        push!(on_args, Get.correlated.visit_occurrence_id .== Get.visit_occurrence_id)
    end
    if !c.ignore_observation_period
        push!(on_args, Fun.and(Get.op_start_date .<= Get.correlated.start_date,
                               Get.correlated.start_date .<= Get.op_end_date))
    end
    push!(on_args, translate(c.start_window, start = true, ignore_observation_period = c.ignore_observation_period))
    if c.end_window !== nothing
        push!(on_args, translate(c.end_window, start = false, ignore_observation_period = c.ignore_observation_period))
    end
    left = !(name === nothing && c.occurrence.type in (AT_LEAST, EXACTLY) && c.occurrence.count > 0)
    q = Join(:correlated => translate(c.criteria),
             Fun.and(args = on_args),
             left = left)
    if c.occurrence.type == AT_LEAST && c.occurrence.count == 1
        q = q |>
            Partition(Get.row_number, order_by = [Get.row_number]) |>
            Where(Agg.row_number() .== 1)
        if name !== nothing
            q = q |>
                Define(name => Fun."is not null"(Get.correlated.event_id))
        end
        return q
    end
    if c.occurrence.type in (EXACTLY, AT_MOST) && c.occurrence.count == 0
        if name !== nothing
            q = q |>
                Partition(Get.row_number, order_by = [Get.row_number]) |>
                Where(Agg.row_number() .== 1) |>
                Define(name => Fun."is null"(Get.correlated.event_id))
        else
            q = q |>
                Where(Fun."is null"(Get.correlated.event_id))
        end
        return q
    end
    q = q |>
        Partition(Get.row_number, order_by = [Get.row_number]) |>
        Where(Agg.row_number() .== 1)
    if c.occurrence.is_distinct
        value = Get.concept_id
        if c.occurrence.count_column == "START_DATE"
            value = Get.start_date
        end
        q = q |>
            Define(:count => SwitchByDialect(cases = [:spark],
                                             branches = [Fun.size(Agg.collect_set(Get.correlated |> value))],
                                             default = Agg.count_distinct(Get.correlated |> value)))
    else
        q = q |>
            Define(:count => Agg.count(Get.correlated.event_id))
    end
    if c.occurrence.type == AT_LEAST
        p = Get.count .>= c.occurrence.count
    elseif c.occurrence.type == AT_MOST
        p = Get.count .<= c.occurrence.count
    elseif c.occurrence.type == EXACTLY
        p = Get.count .== c.occurrence.count
    end
    if name !== nothing
        q = q |>
            Define(name => p)
    else
        q = q |>
            Where(p)
    end
    q
end

function translate(w::Window; start::Bool, ignore_observation_period::Bool)
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
    Fun.and(args = args)
end

function translate(c::CollapseSettings)
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

function translate(c::ConceptSet)
    include = ConceptSetItem[]
    exclude = ConceptSetItem[]
    for item in c.items
        if !item.is_excluded
            push!(include, item)
        else
            push!(exclude, item)
        end
    end
    q = translate(include)
    if !isempty(exclude)
        q = q |>
            LeftJoin(:excluded => translate(exclude),
                     Get.concept_id .== Get.excluded.concept_id) |>
            Where(Fun."is null"(Get.excluded.concept_id))
    end
    q = q |>
        Select(Get.concept_id)
    q = With("concept_set_$(c.id)" => q)
    q
end

function translate(items::Vector{ConceptSetItem}; skip_mapped = false)
    # TODO: Fun.in
    args = SQLNode[item.concept.concept_id for item in items]
    q = From(:concept) |>
        Where(Fun.in(args = SQLNode[Get.concept_id, args...]))
    with_descendants = [item for item in items if item.include_descendants]
    if !isempty(with_descendants)
        args = [item.concept.concept_id for item in with_descendants]
        q = q |>
            Append(
                From(:concept) |>
                Where(Fun."is null"(Get.invalid_reason)) |>
                Join(From(:concept_ancestor),
                     Get.concept_id .== Get.descendant_concept_id) |>
                Where(Fun.in(args = SQLNode[Get.ancestor_concept_id, args...]))) |>
            Group(Get.concept_id)
    end
    with_mapped = [item for item in items if item.include_mapped]
    if !isempty(with_mapped) && !skip_mapped
        q = q |>
            Append(
                translate(with_mapped, skip_mapped = true) |>
                Join(From(:concept_relationship) |>
                     Where(Fun.and(Fun."is_null"(Get.invalid_reason),
                                   Get.relationship_id .== "Maps to")),
                     Get.concept_id .== Get.concept_id_2) |>
                 Define(:concept_id => Get.concept_id_1))
    end
    q
end

function translate(r::Union{NumericRange, DateRange})
    if r.op == GT
        Var.field .> r.value
    elseif r.op == GTE
        Var.field .>= r.value
    elseif r.op == LT
        Var.field .< r.value
    elseif r.op == LTE
        Var.field .<= r.value
    elseif r.op == EQ
        Var.field .== r.value
    elseif r.op == NEQ
        Var.field .!= r.value
    elseif r.op == BT
        Fun.between(Var.field, r.value, r.extent)
    elseif r.op == NBT
        Fun."not between"(Var.field, r.value, r.extent)
    end
end

