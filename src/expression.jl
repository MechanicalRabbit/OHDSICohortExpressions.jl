using Dates
using PrettyPrinting: PrettyPrinting, @isexpr

import Base: isempty, parse

macro unpack(ex)
    if @isexpr ex Expr(:struct, mut::Bool, decl, Expr(:block, args...))
        if @isexpr decl Expr(:(<:), T, _)
        else
            T = decl
        end
        struct_slots = Any[]
        ctr_slots = Any[]
        new_slots = Any[]
        unpack_slots = Any[]
        quoteof_slots = Any[]
        for arg in args
            if arg isa LineNumberNode
                push!(struct_slots, arg)
                continue
            end
            if @isexpr arg Expr(:(=), arg′, default)
                arg = arg′
                has_default = true
            else
                has_default = false
            end
            if @isexpr arg Expr(:call, :(=>), key, arg′)
                arg = arg′
                has_key = true
            else
                has_key = false
            end
            if @isexpr arg Expr(:(::), name, FT)
            else
                error("expected field declaration; got $(repr(arg))")
            end
            @isexpr FT Expr(:curly, :Union, FT, _)
            push!(struct_slots, arg)
            push!(ctr_slots, has_default ? Expr(:kw, name, default) : name)
            push!(new_slots, name)
            unpack_slot = Expr(:call, :unpack!, FT, :data)
            if has_key
                push!(unpack_slot.args, key)
                if has_default
                    push!(unpack_slot.args, default)
                end
            end
            unpack_slot = Expr(:kw, name, unpack_slot)
            push!(unpack_slots, unpack_slot)
            quoteof_slot = :(push!(ex.args, Expr(:kw, $(QuoteNode(name)), obj.$name)))
            if has_default
                quoteof_slot = :(obj.$name == $default || $quoteof_slot)
            end
            push!(quoteof_slots, quoteof_slot)
        end
        return quote
            struct $decl
                $(struct_slots...)
                $T(; $(ctr_slots...)) = new($(new_slots...))
            end
            unpack!(::Type{$T}, data::Dict) = $T($(unpack_slots...))
            function PrettyPrinting.quoteof(obj::$T)
                ex = Expr(:call, nameof($T))
                $(quoteof_slots...)
                ex
            end
        end |> esc
    else
        error("expected a struct; got $(repr(ex))")
    end
end

unpack!(T::Type, data::Dict, key::String, default) =
    haskey(data, key) ?
        something(unpack!(T, data, key), default) :
        default

function unpack!(T::Type, data::Dict, key::String)
    bucket = data[key]
    retval = unpack!(T, bucket)
    if !(bucket isa Union{Dict,Vector}) || isempty(bucket)
        delete!(data, key)
    end
    return retval
end

unpack!(::Type{String}, data) =
    data

function unpack!(T::Type{<:Union{Date, Number, Enum}}, data)
    if data isa String
        return parse(T, data)
    end
    return T(data)
end

function unpack!(::Type{Vector{T}}, items::Vector{Any}) where {T}
    retval = T[]
    for item in items
        push!(retval, unpack!(T, item))
    end
    filter!(item -> !isempty(item), items)
    return retval
end

@enum RangeOp GT GTE LT LTE EQ NEQ BT NBT
Base.parse(::Type{RangeOp}, s::String) =
    s == "gt" ? GT :
    s == "gte" ? GTE :
    s == "lt" ? LT :
    s == "lte" ? LTE :
    s == "eq" ? EQ :
    s == "!eq" ? NEQ :
    s == "bt" ? BT :
    s == "!bt" ? NBT :
    throw(DomainError(s, "Unknown Range Operation"))

@enum TextOp CONTAINS NCONTAINS STARTSWITH NSTARTSWITH ENDSWITH NENDSWITH
Base.parse(::Type{TextOp}, s::String) =
    s == "contains" ? CONTAINS :
    s == "!contains" ? NCONTAINS :
    s == "startsWith" ? STARTSWITH :
    s == "!startsWith" ? NSTARTSWITH :
    s == "endsWith" ? ENDSWITH :
    s == "!endsWith" ? NENDSWITH :
    throw(DomainError(s, "Unknown Text Operation"))

@unpack struct DateRange
    "Value" => value::Date
    "Op" => op::RangeOp
    "Extent" => extent::Union{Date, Nothing} = nothing
end

@unpack struct TextFilter
    "Text" => text::String = "null"
    "Op" => op::TextOp
end

@unpack struct NumericRange
    "Value" => value::Number
    "Op" => op::RangeOp
    "Extent" => extent::Union{Number, Nothing} = nothing
end

@enum InvalidReasonFlag UNKNOWN_REASON VALID INVALID
InvalidReasonFlag(::Nothing) = UNKNOWN_REASON
Base.parse(::Type{InvalidReasonFlag}, s::Union{String, Nothing}) =
    s == "V" ? VALID :
    s == "D" ? INVALID :
    s == "U" ? INVALID :
    isnothing(s) ? UNKNOWN_REASON :
         throw(DomainError(s, "Unknown Invalid Reason Flag"))

@enum StandardConceptFlag UNKNOWN_STANDARD STANDARD NON_STANDARD CLASSIFICATION
StandardConceptFlag(::Nothing) = UNKNOWN_STANDARD
Base.parse(::Type{StandardConceptFlag}, s::Union{String, Nothing}) =
    s == "N" ? NON_STANDARD :
    s == "S" ? STANDARD :
    s == "C" ? CLASSIFICATION :
    isnothing(s) ? UNKNOWN_STANDARD :
         throw(DomainError(s, "Unknown Standard Concept Flag"))

@unpack struct Concept
    "CONCEPT_CLASS_ID" => concept_class_id::String = ""
    "CONCEPT_CODE" => concept_code::String
    "CONCEPT_ID" => concept_id::Int
    "CONCEPT_NAME" => concept_name::String
    "DOMAIN_ID" => domain_id::String
    "INVALID_REASON" => invalid_reason::InvalidReasonFlag = UNKNOWN_REASON
    "INVALID_REASON_CAPTION" => invalid_reason_caption::String
    "STANDARD_CONCEPT" => standard_concept::StandardConceptFlag = UNKNOWN_STANDARD
    "STANDARD_CONCEPT_CAPTION" => standard_concept_caption::String
    "VOCABULARY_ID" => vocabulary_id::String
end

abstract type Criteria end

function Base.getproperty(obj::Criteria, prop::Symbol)
    if prop in fieldnames(BaseCriteria)
        return getfield(obj.base, prop)
    else
        return getfield(obj, prop)
    end
end

@unpack struct Endpoint
    "Days" => days::Union{Int, Nothing} = nothing
    "Coeff" => coeff::Int
end

@unpack struct Window
    "Start" => start::Endpoint
    "End" => end_::Endpoint
    "UseIndexEnd" => use_index_end::Union{Bool, Nothing} = nothing
    "UseEventEnd" => use_event_end::Union{Bool, Nothing} = nothing
end

@enum OccurrenceType EXACTLY=0 AT_MOST=1 AT_LEAST=2
Base.parse(::Type{OccurrenceType}, s::String) =
    s == "0" ? EXACTLY :
    s == "1" ? AT_MOST :
    s == "2" ? AT_LEAST :
         throw(DomainError(s, "Unknown Occurrence Type"))

@unpack struct Occurrence
    "Type" => type::OccurrenceType
    "Count" => count::Int
    "IsDistinct" => is_distinct::Bool = false
    "CountColumn" => count_column::Union{String, Nothing} = nothing
end

@unpack struct CorrelatedCriteria
    "Criteria" => criteria::Union{Criteria, Nothing} = nothing
    "EndWindow" => end_window::Union{Window, Nothing} = nothing
    "IgnoreObservationPeriod" => ignore_observation_period::Bool = false
    "Occurrence" => occurrence::Union{Occurrence, Nothing} = nothing
    "RestrictVisit" => restrict_visit::Bool = false
    "StartWindow" => start_window::Window
end

@unpack struct DemographicCriteria
    "Age" => age::Union{NumericRange, Nothing} = nothing
    "Ethnicity" => ethnicity::Vector{Concept} = Concept[]
    "Gender" => gender::Vector{Concept} = Concept[]
    "OccurrenceEndDate" => occurrence_end_date::Union{DateRange, Nothing} = nothing
    "OccurrenceStartDate" => occurrence_start_date::Union{DateRange, Nothing} = nothing
    "Race" => race::Vector{Concept} = Concept[]
end

@enum CriteriaGroupType ALL_CRITERIA ANY_CRITERIA AT_LEAST_CRITERIA AT_MOST_CRITERIA
Base.parse(::Type{CriteriaGroupType}, s::String) =
    s == "ALL" ? ALL_CRITERIA :
    s == "ANY" ? ANY_CRITERIA :
    s == "AT_LEAST" ? AT_LEAST_CRITERIA :
    s == "AT_MOST" ? AT_MOST_CRITERIA :
    throw(DomainError(s, "Unknown Criteria Group Type"))

@unpack struct CriteriaGroup
    "Count" => count::Union{Int, Nothing} = nothing
    "CriteriaList" => correlated_criteria::Vector{CorrelatedCriteria} = CorrelatedCriteria[]
    "DemographicCriteriaList" => demographic_criteria::Vector{DemographicCriteria} = DemographicCriteria[]
    "Groups" => groups::Vector{CriteriaGroup} = CriteriaGroup[]
    "Type" => type::CriteriaGroupType
end

isempty(g::CriteriaGroup) =
    isempty(g.correlated_criteria) &&
    isempty(g.demographic_criteria) &&
    isempty(g.groups)

@enum CollapseType UNKNOWN_COLLAPSE ERA
CollapseType(::Nothing) = UNKNOWN_COLLAPSE
Base.parse(::Type{CollapseType}, s::Union{String, Nothing}) =
    s == "ERA" ? ERA :
    isnothing(s) ? UNKNOWN_COLLAPSE :
         throw(DomainError(s, "Unknown Collapse Type"))

@unpack struct CollapseSettings
    "CollapseType" => collapse_type::CollapseType
    "EraPad" => era_pad::Int = 0
end

@unpack struct Period
    "StartDate" => start_date::Union{Date, Nothing} = nothing
    "EndDate" => end_date::Union{Date, Nothing} = nothing
end

@unpack struct ConceptSetItem
    "concept" => concept::Concept
    "isExcluded" => is_excluded::Bool = false
    "includeDescendants" => include_descendants::Bool = false
    "includeMapped" => include_mapped::Bool = false
end

function unpack!(T::Type{Vector{ConceptSetItem}}, data::Dict)
    items = data["items"]
    retval = unpack!(T, items)
    if isempty(items)
        delete!(data, "items")
    end
    return retval
end

@unpack struct ConceptSet
    "id" => id::Int
    "name" => name::String
    "expression" => items::Vector{ConceptSetItem} = ConceptSetItem[]
end

abstract type EndStrategy end

@unpack struct CustomEraStrategy <: EndStrategy
    "DrugCodesetId" => drug_codeset_id::Union{Int, Nothing} = nothing
    "GapDays" => gap_days::Int = 0
    "Offset" => offset::Int = 0
    "DaysSupplyOverride" => days_supply_override::Union{Int, Nothing} = nothing
end

@enum DateField START_DATE END_DATE
Base.parse(::Type{DateField}, s::String) =
    s == "StartDate" ? START_DATE :
    s == "EndDate" ? END_DATE :
    throw(DomainError(s, "Unknown Date Field"))

@unpack struct DateOffsetStrategy <: EndStrategy
    "Offset" => offset::Integer
    "DateField" => date_field::DateField
end

function unpack!(::Type{EndStrategy}, data::Dict)
    if haskey(data, "DateOffset")
        (key, type) = ("DateOffset", DateOffsetStrategy)
    else
        (key, type) = ("CustomEra", CustomEraStrategy)
    end
    subdata = data[key]
    retval = unpack!(type, subdata)
    if isempty(subdata)
        delete!(data, key)
    end
    return retval
end

@unpack struct InclusionRule
    "name" => name::String
    "description" => description::String = ""
    "expression" => expression::CriteriaGroup
end

@unpack struct ObservationFilter
    "PriorDays" => prior_days::Int = 0
    "PostDays" => post_days::Int = 0
end

@enum ResultLimitType FIRST LAST ALL
Base.parse(::Type{ResultLimitType}, s::Union{String, Nothing}) =
    s == "First" ? FIRST :
    s == "Last" ? LAST :
    s == "All" ? ALL :
    isnothing(s) ? FIRST :
        throw(DomainError(s, "Unknown Result Limit Type"))

@unpack struct ResultLimit
    "Type" => type::ResultLimitType = FIRST
end

@unpack struct PrimaryCriteria
    "CriteriaList" => criteria_list::Vector{Criteria}
    "ObservationWindow" => observation_window::ObservationFilter
    "PrimaryCriteriaLimit" => primary_limit::ResultLimit
end

@unpack struct BaseCriteria
    "Age" => age::Union{NumericRange, Nothing} = nothing
    "CodesetId" => codeset_id::Union{Int, Nothing} = nothing
    "CorrelatedCriteria" => correlated_criteria::Union{CriteriaGroup, Nothing} = nothing
    "First" => first::Bool = false
    "Gender" => gender::Vector{Concept} = Concept[]
    "OccurrenceEndDate" => occurrence_end_date::Union{DateRange, Nothing} = nothing
    "OccurrenceStartDate" => occurrence_start_date::Union{DateRange, Nothing} = nothing
    "ProviderSpecialty" => provider_specialty::Vector{Concept} = Concept[]
    "VisitType" => visit_type::Vector{Concept} = Concept[]
end

struct UnknownCriteria <: Criteria
end

unpack!(::Type{UnknownCriteria}, data::Dict) = UnknownCriteria()

PrettyPrinting.quoteof(obj::UnknownCriteria) =
    Expr(:call, nameof(UnknownCriteria))

@unpack struct ConditionEra <: Criteria
    # like DrugEra, but missing gap_length?
    base::BaseCriteria
    "EraEndDate" => era_end_date::Union{DateRange, Nothing} = nothing
    "EraStartDate" => era_start_date::Union{DateRange, Nothing} = nothing
    "EraLength" => era_length::Union{NumericRange, Nothing} = nothing
    "OccurrenceCount" => occurrence_count::Union{NumericRange, Nothing} = nothing
    "AgeAtStart" => age_at_start::Union{NumericRange, Nothing} = nothing
    "AgeAtEnd" => age_at_end::Union{NumericRange, Nothing} = nothing
end

@unpack struct ConditionOccurrence <: Criteria
    base::BaseCriteria
    "ConditionSourceConcept" => condition_source_concept::Union{Int, Nothing} = nothing
    "ConditionStatus" => condition_status::Vector{Concept} = Concept[]
    "ConditionType" => condition_type::Vector{Concept} = Concept[]
    "ConditionTypeExclude" => condition_type_exclude::Bool = false
    "StopReason" => stop_reason::Union{TextFilter, Nothing} = nothing
end

@unpack struct Death <: Criteria
    base::BaseCriteria
    "DeathSourceConcept" => death_source_concept::Union{Int, Nothing} = nothing
    "DeathType" => death_type::Vector{Concept} = Concept[]
    "DeathTypeExclude" => death_type_exclude::Bool = false
end

@unpack struct DeviceExposure <: Criteria
    base::BaseCriteria
    "DeviceSourceConcept" => device_source_concept::Union{Int, Nothing} = nothing
    "DeviceType" => device_type::Vector{Concept} = Concept[]
    "DeviceTypeExclude" => device_type_exclude::Bool = false
    "Quantity" => quantity::Union{NumericRange, Nothing} = nothing
    "UniqueDeviceId" => unique_device_id::Union{TextFilter, Nothing} = nothing
end

@unpack struct DrugEra <: Criteria
    base::BaseCriteria
    "EraEndDate" => era_end_date::Union{DateRange, Nothing} = nothing
    "EraStartDate" => era_start_date::Union{DateRange, Nothing} = nothing
    "EraLength" => era_length::Union{NumericRange, Nothing} = nothing
    "OccurrenceCount" => occurrence_count::Union{NumericRange, Nothing} = nothing
    "GapDays" => gap_days::Union{NumericRange, Nothing} = nothing
    "AgeAtStart" => age_at_start::Union{NumericRange, Nothing} = nothing
    "AgeAtEnd" => age_at_end::Union{NumericRange, Nothing} = nothing
end

@unpack struct DrugExposure <: Criteria
    base::BaseCriteria
    "DrugSourceConcept" => drug_source_concept::Union{Int, Nothing} = nothing
    "DrugType" => drug_type::Vector{Concept} = Concept[]
    "DrugTypeExclude" => drug_type_exclude::Bool = false
    "Refills" => refills::Union{NumericRange, Nothing} = nothing
    "Quantity" => quantity::Union{NumericRange, Nothing} = nothing
    "DaysSupply" => days_supply::Union{NumericRange, Nothing} = nothing
    "RouteConcept" => route_concept::Vector{Concept} = Concept[]
    "EffectiveDrugDose" => effective_drug_dose::Union{NumericRange, Nothing} = nothing
    "DoseUnit" => dose_unit::Vector{Concept} = Concept[]
    "LotNumber" => lot_number::Union{TextFilter, Nothing} = nothing
    "StopReason" => stop_reason::Union{TextFilter, Nothing} = nothing
end

@unpack struct DoseEra <: Criteria
    base::BaseCriteria
    "DoseValue" => dose_value::Union{NumericRange, Nothing} = nothing
    "EraEndDate" => era_end_date::Union{DateRange, Nothing} = nothing
    "EraStartDate" => era_start_date::Union{DateRange, Nothing} = nothing
    "EraLength" => era_length::Union{NumericRange, Nothing} = nothing
    "AgeAtStart" => age_at_start::Union{NumericRange, Nothing} = nothing
    "AgeAtEnd" => age_at_end::Union{NumericRange, Nothing} = nothing
    "Unit" => unit::Vector{Concept} = Concept[]
end

@unpack struct LocationRegion <: Criteria
    "CodesetId" => codeset_id::Union{Int, Nothing} = nothing
    "StartDate" => start_date::Union{DateRange, Nothing} = nothing
    "EndDate" => end_date::Union{DateRange, Nothing} = nothing
end

Base.getproperty(obj::LocationRegion, prop::Symbol) =
    getfield(obj, prop)

@unpack struct Measurement <: Criteria
    base::BaseCriteria
    "MeasurementSourceConcept" => measurement_source_concept::Union{Int, Nothing} = nothing
    "MeasurementType" => measurement_type::Vector{Concept} = Concept[]
    "MeasurementTypeExclude" => measurement_type_exclude::Bool = false
    "Abnormal" => abnormal::Union{Bool, Nothing} = nothing
    "RangeLow" => range_low::Union{NumericRange, Nothing} = nothing
    "RangeHigh" => range_high::Union{NumericRange, Nothing} = nothing
    "RangeLowRatio" => range_low_ratio::Union{NumericRange, Nothing} = nothing
    "RangeHighRatio" => range_high_ratio::Union{NumericRange, Nothing} = nothing
    "ValueAsNumber" => value_as_number::Union{NumericRange, Nothing} = nothing
    "ValueAsConcept" => value_as_concept::Vector{Concept} = Concept[]
    "Operator" => operator::Vector{Concept} = Concept[]
    "Unit" => unit::Vector{Concept} = Concept[]
end

@unpack struct Observation <: Criteria
    base::BaseCriteria
    "ObservationSourceConcept" => observation_source_concept::Union{Int, Nothing} = nothing
    "ObservationType" => observation_type::Vector{Concept} = Concept[]
    "ObservationTypeExclude" => observation_type_exclude::Bool = false
    "ValueAsString" => value_as_string::Union{TextFilter, Nothing} = nothing
    "ValueAsNumber" => value_as_number::Union{NumericRange, Nothing} = nothing
    "ValueAsConcept" => value_as_concept::Vector{Concept} = Concept[]
    "Qualifier" => qualifier::Vector{Concept} = Concept[]
    "Unit" => unit::Vector{Concept} = Concept[]
end

@unpack struct ObservationPeriod <: Criteria
    base::BaseCriteria
    "PeriodType" => period_type::Vector{Concept} = Concept[]
    "PeriodTypeExclude" => period_type_exclude::Bool = false
    "PeriodStartDate" => period_start_date::Union{DateRange, Nothing} = nothing
    "PeriodEndDate" => period_end_date::Union{DateRange, Nothing} = nothing
    "PeriodLength" => period_length::Union{NumericRange, Nothing} = nothing
    "AgeAtStart" => age_at_start::Union{NumericRange, Nothing} = nothing
    "AgeAtEnd" => age_at_end::Union{NumericRange, Nothing} = nothing
    "UserDefinedPeriod" => user_defined_period::Union{Period, Nothing} = nothing
end

@unpack struct PayerPlanPeriod <: Criteria
    base::BaseCriteria
    "PeriodType" => period_type::Vector{Concept} = Concept[]
    "PeriodTypeExclude" => period_type_exclude::Bool = false
    "PeriodStartDate" => period_start_date::Union{DateRange, Nothing} = nothing
    "PeriodEndDate" => period_end_date::Union{DateRange, Nothing} = nothing
    "PeriodLength" => period_length::Union{NumericRange, Nothing} = nothing
    "AgeAtStart" => age_at_start::Union{NumericRange, Nothing} = nothing
    "AgeAtEnd" => age_at_end::Union{NumericRange, Nothing} = nothing
    "PayerConcept" => payer_concept::Union{Int, Nothing} = nothing
    "PlanConcept" => plan_concept::Union{Int, Nothing} = nothing
    "SponsorConcept" => sponsor_concept::Union{Int, Nothing} = nothing
    "StopReasonConcept" => stop_reason_concept::Union{Int, Nothing} = nothing
    "StopReasonSourceConcept" => stop_reason_source_concept::Union{Int, Nothing} = nothing
    "PayerSourceConcept" => payer_source_concept::Union{Int, Nothing} = nothing
    "PlanSourceConcept" => plan_source_concept::Union{Int, Nothing} = nothing
    "SponsorSourceConcept" => sponsor_source_concept::Union{Int, Nothing} = nothing
    "UserDefinedPeriod" => user_defined_period::Union{Period, Nothing} = nothing
end

@unpack struct ProcedureOccurrence <: Criteria
    base::BaseCriteria
    "ProcedureSourceConcept" => procedure_source_concept::Union{Int, Nothing} = nothing
    "ProcedureType" => procedure_type::Vector{Concept} = Concept[]
    "ProcedureTypeExclude" => procedure_type_exclude::Bool = false
    "Modifier" => modifier::Vector{Concept} = Concept[]
    "Quantity" => quantity::Union{NumericRange, Nothing} = nothing
end

@unpack struct Specimen <: Criteria
    base::BaseCriteria
    "SpecimenSourceConcept" => specimen_source_concept::Union{Int, Nothing} = nothing
    "SpecimenType" => specimen_type::Vector{Concept} = Concept[]
    "SpecimenTypeExclude" => specimen_type_exclude::Bool = false
    "Quantity" => quantity::Union{NumericRange, Nothing} = nothing
    "Unit" => unit::Vector{Concept} = Concept[]
    "AnatomicSite" => anatomic_site::Vector{Concept} = Concept[]
    "DiseaseStatus" => disease_status::Vector{Concept} = Concept[]
    "SourceId" => source_id::Union{TextFilter, Nothing} = nothing
end

@unpack struct VisitOccurrence <: Criteria
    base::BaseCriteria
    "PlaceOfService" => place_of_service::Vector{Concept} = Concept[]
    "PlaceOfServiceLocation" => place_of_service_location::Union{Int, Nothing} = nothing
    "VisitSourceConcept" => visit_source_concept::Union{Int, Nothing} = nothing
    "VisitLength" => visit_length::Union{NumericRange, Nothing} = nothing
    "VisitTypeExclude" => visit_type_exclude::Bool = false
end

function unpack!(::Type{Criteria}, data::Dict)
    for type in (ConditionEra, ConditionOccurrence, Death,
                 DeviceExposure, DoseEra, DrugEra, DrugExposure,
                 LocationRegion, Measurement, Observation,
                 ObservationPeriod, PayerPlanPeriod,
                 ProcedureOccurrence, Specimen, VisitOccurrence)
        key = string(nameof(type))
        if haskey(data, key)
            subdata = data[key]
            retval = unpack!(type, subdata)
            if isempty(subdata)
                delete!(data, key)
            end
            return retval
        end
    end
    return unpack!(UnknownCriteria, data)
end

@unpack struct CohortExpression
    "AdditionalCriteria" => additional_criteria::Union{CriteriaGroup, Nothing} = nothing
    "CensorWindow" => censor_window::Union{Period, Nothing} = nothing
    "CensoringCriteria" => censoring_criteria::Vector{Criteria} = Criteria[]
    "CollapseSettings" => collapse_settings::CollapseSettings
    "ConceptSets" => concept_sets::Vector{ConceptSet} = ConceptSet[]
    "EndStrategy" => end_strategy::Union{EndStrategy, Nothing} = nothing
    "ExpressionLimit" => expression_limit::ResultLimit
    "InclusionRules" => inclusion_rules::Vector{InclusionRule} = InclusionRule[]
    "PrimaryCriteria" => primary_criteria::PrimaryCriteria
    "QualifiedLimit" => qualified_limit::ResultLimit
    "Title" => title::Union{String, Nothing} = nothing
    "cdmVersionRange" => version_range::Union{String, Nothing} = nothing
end

unpack!(data) = unpack!(CohortExpression, data)

