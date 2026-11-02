param(
    [string]$RepoPath = "D:\harness\projects\publicofferingshares\ipo-data",
    [Parameter(Mandatory = $true)]
    [string]$CandidateLedgerPath,
    [string]$AsOfDate,
    [Alias("RangeStart")][string]$RequestedRangeStart,
    [string]$RangeEndExclusive
)

$ErrorActionPreference = "Stop"

function Add-Issue {
    param([string]$Level, [string]$Code, [string]$Message)
    $script:Issues.Add([pscustomobject]@{ Level = $Level; Code = $Code; Message = $Message })
}

function Read-Json {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { throw "Missing JSON file: $Path" }
    Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
}

function Has-Value {
    param($Value)
    if ($null -eq $Value) { return $false }
    if ($Value -is [string]) { return -not [string]::IsNullOrWhiteSpace($Value) }
    return $true
}

function Require-Value {
    param($Object, [string]$Property, [string]$EventId, [string]$Phase)
    if ($null -eq $Object -or -not (Has-Value $Object.$Property)) {
        Add-Issue "ERROR" "missing_$Property" "$EventId is missing $Property ($Phase)."
    }
}

function Parse-DateOrNull {
    param($Value)
    if (-not (Has-Value $Value)) { return $null }
    $parsed = [datetime]::MinValue
    if (-not [datetime]::TryParseExact([string]$Value, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::None, [ref]$parsed)) { return $null }
    $parsed.Date
}

function Add-BusinessDays {
    param([datetime]$Date, [int]$Days)
    $cursor = $Date.Date
    $remaining = $Days
    while ($remaining -gt 0) {
        $cursor = $cursor.AddDays(1)
        if ($cursor.DayOfWeek -notin @([DayOfWeek]::Saturday, [DayOfWeek]::Sunday)) {
            $remaining -= 1
        }
    }
    $cursor
}

$RepoPath = (Resolve-Path -LiteralPath $RepoPath).Path
if (-not [IO.Path]::IsPathRooted($CandidateLedgerPath)) {
    $CandidateLedgerPath = Join-Path $RepoPath $CandidateLedgerPath
}

$now = if (Has-Value $AsOfDate) {
    $parsedAsOf = Parse-DateOrNull $AsOfDate
    if ($null -eq $parsedAsOf) { throw "AsOfDate must be YYYY-MM-DD." }
    $parsedAsOf
} else {
    try { $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Korea Standard Time") }
    catch { $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Asia/Seoul") }
    [TimeZoneInfo]::ConvertTime([DateTimeOffset]::UtcNow, $tz).Date
}

$requestedStart = $RequestedRangeStart
$requestedEnd = $RangeEndExclusive
if ((Has-Value $requestedStart) -ne (Has-Value $requestedEnd)) { throw "Supply both RangeStart and RangeEndExclusive." }
if (Has-Value $requestedStart) {
    $rangeStart = Parse-DateOrNull $requestedStart
    $rangeEnd = Parse-DateOrNull $requestedEnd
    if ($null -eq $rangeStart -or $null -eq $rangeEnd -or $rangeStart.Day -ne 1 -or $rangeEnd.Day -ne 1 -or $rangeEnd -le $rangeStart) { throw "Range must be increasing ISO month boundaries." }
} else {
    $rangeStart = (Get-Date -Year $now.Year -Month $now.Month -Day 1).Date
    $rangeEnd = $rangeStart.AddMonths(2)
}
$Issues = [Collections.Generic.List[object]]::new()
$ledger = Read-Json $CandidateLedgerPath
$eventsDoc = Read-Json (Join-Path $RepoPath "data\discovered\ipo_events.json")
$manual = Read-Json (Join-Path $RepoPath "data\manual_fundamentals.json")
$yearDocs = @{}
foreach ($year in $rangeStart.Year..$rangeEnd.AddDays(-1).Year) {
    $yearPath = Join-Path $RepoPath "ipo_competition_data\yearly\$year.json"
    if (Test-Path -LiteralPath $yearPath) { $yearDocs[$year] = Read-Json $yearPath }
}

$ledgerStart = Parse-DateOrNull $ledger.rangeStart
$ledgerEnd = Parse-DateOrNull $ledger.rangeEndExclusive
if ($ledgerStart -ne $rangeStart.Date -or $ledgerEnd -ne $rangeEnd.Date) {
    Add-Issue "ERROR" "ledger_range" "Ledger range must be $($rangeStart.ToString('yyyy-MM-dd')) through $($rangeEnd.ToString('yyyy-MM-dd')) exclusive."
}
if (@($ledger.sources).Count -lt 3) {
    Add-Issue "ERROR" "ledger_sources" "Candidate ledger must contain at least three independent schedule sources."
}

$validStatuses = @("matched", "added", "excluded", "unresolved")
$candidates = @($ledger.candidates)
foreach ($candidate in $candidates) {
    if ($candidate.status -notin $validStatuses) { Add-Issue "ERROR" "ledger_status" "$($candidate.company) has invalid ledger status '$($candidate.status)'." }
    if ($candidate.status -eq "unresolved") { Add-Issue "ERROR" "unresolved_candidate" "$($candidate.company) remains unresolved." }
    if ($candidate.status -eq "excluded" -and -not (Has-Value $candidate.notes)) { Add-Issue "ERROR" "exclusion_reason" "$($candidate.company) is excluded without a reason." }
    if ($candidate.status -in @("matched", "added") -and @($candidate.evidenceUrls).Count -eq 0) { Add-Issue "ERROR" "candidate_evidence" "$($candidate.company) has no evidence URL." }
}

$includedCandidates = @($candidates | Where-Object { $_.status -in @("matched", "added") })
$events = @($eventsDoc.stocks | Where-Object {
    $date = Parse-DateOrNull $_.subscriptionStart
    $null -ne $date -and $date -ge $rangeStart -and $date -lt $rangeEnd
})

foreach ($candidate in $includedCandidates) {
    $sourceMatch = @($events | Where-Object { $_.company -ceq $candidate.company -and $_.subscriptionStart -eq $candidate.subscriptionStart -and $_.subscriptionEnd -eq $candidate.subscriptionEnd })
    if ($sourceMatch.Count -eq 0 -and (Has-Value $candidate.eventId)) {
        $candidateStockPath = Join-Path $RepoPath "ipo_competition_data\stocks\$($candidate.eventId).json"
        if (Test-Path -LiteralPath $candidateStockPath) {
            $candidateStock = Read-Json $candidateStockPath
            if ($candidateStock.company -ceq $candidate.company -and $candidateStock.subscriptionStart -eq $candidate.subscriptionStart -and $candidateStock.subscriptionEnd -eq $candidate.subscriptionEnd) {
                $events += $candidateStock
            }
        }
    }
}

foreach ($duplicate in $events | Group-Object id | Where-Object Count -gt 1) { Add-Issue "ERROR" "duplicate_id" "Duplicate event id: $($duplicate.Name)." }
foreach ($duplicate in $events | Group-Object { "$($_.company)|$($_.subscriptionStart)|$($_.subscriptionEnd)" } | Where-Object Count -gt 1) { Add-Issue "ERROR" "duplicate_window" "Duplicate company/subscription window: $($duplicate.Name)." }

foreach ($candidate in $includedCandidates) {
    $matches = @($events | Where-Object { $_.company -ceq $candidate.company -and $_.subscriptionStart -eq $candidate.subscriptionStart -and $_.subscriptionEnd -eq $candidate.subscriptionEnd })
    if ($matches.Count -ne 1) { Add-Issue "ERROR" "candidate_match" "$($candidate.company) must match exactly one local event; found $($matches.Count)." }
    elseif ((Has-Value $candidate.eventId) -and $matches[0].id -ne $candidate.eventId) { Add-Issue "ERROR" "candidate_id" "$($candidate.company) ledger id '$($candidate.eventId)' differs from '$($matches[0].id)'." }
}

foreach ($event in $events) {
    $id = [string]$event.id
    if (@($includedCandidates | Where-Object eventId -CEQ $id).Count -ne 1) { Add-Issue "ERROR" "unreviewed_event" "$id is not represented exactly once in the included ledger." }
    foreach ($field in @("subscriptionStart", "subscriptionEnd", "refundDate", "listingDate", "demandForecastStart", "demandForecastEnd")) {
        if ((Has-Value $event.$field) -and $null -eq (Parse-DateOrNull $event.$field)) { Add-Issue "ERROR" "invalid_date" "$id has invalid $field." }
    }
    if ($event.company -match "&(?:amp|#\d+|#x[0-9A-Fa-f]+);") { Add-Issue "ERROR" "html_entity" "$id contains an HTML entity in company." }
    foreach ($field in @("id", "company", "market", "industry", "subscriptionStart", "subscriptionEnd")) { Require-Value $event $field $id "baseline" }
    $hasDemandForecastSchedule = (Has-Value $event.demandForecastDate) -or (Has-Value $event.demandForecastStart) -or (Has-Value $event.demandForecastEnd)
    if ($hasDemandForecastSchedule) {
        foreach ($field in @("demandForecastStart", "demandForecastEnd")) { Require-Value $event $field $id "announced demand schedule" }
    }
    if (@($event.leadManagers).Count -eq 0) { Add-Issue "ERROR" "missing_leadManagers" "$id has no lead manager." }

    $subscriptionStart = Parse-DateOrNull $event.subscriptionStart
    $subscriptionEnd = Parse-DateOrNull $event.subscriptionEnd
    $demandStart = Parse-DateOrNull $event.demandForecastStart
    $demandEnd = Parse-DateOrNull $event.demandForecastEnd
    if ($null -ne $subscriptionStart -and $now -ge $subscriptionStart) { Require-Value $event "refundDate" $id "subscription" }
    if (-not (Has-Value $event.listingDate)) { Add-Issue "WARNING" "listing_date" "$id has no announced listing date." }
    if ($null -ne $subscriptionStart -and $null -ne $subscriptionEnd -and $subscriptionEnd -lt $subscriptionStart) { Add-Issue "ERROR" "subscription_range" "$id subscription end precedes start." }
    if ($null -ne $demandStart -and $null -ne $demandEnd -and $demandEnd -lt $demandStart) { Add-Issue "ERROR" "demand_range" "$id demand forecast end precedes start." }

    $stockPath = Join-Path $RepoPath "ipo_competition_data\stocks\$id.json"
    if (-not (Test-Path -LiteralPath $stockPath)) { Add-Issue "ERROR" "missing_stock" "$id has no generated stock file."; continue }
    $stock = Read-Json $stockPath
    if ($stock.id -ne $id -or $stock.company -cne $event.company -or $stock.subscriptionStart -ne $event.subscriptionStart) { Add-Issue "ERROR" "stock_identity" "$id generated stock identity differs from the source event." }

    $fundamentals = $stock.fundamentals
    foreach ($field in @("priceBandMin", "priceBandMax", "publicAllocationShares")) { Require-Value $fundamentals $field $id "announced baseline" }
    $demandResultDeadline = if ($null -ne $demandEnd) { Add-BusinessDays -Date $demandEnd -Days 1 } else { $null }
    if ($null -ne $demandResultDeadline -and $now -gt $demandResultDeadline) {
        foreach ($field in @("offerPrice", "topBandConfirmation", "institutionCompetitionRate", "institutionParticipants", "lockupCommitmentRate", "floatRate", "marketCapKrw", "publicAllocationShares", "hasPutbackRight", "putbackSummary")) { Require-Value $fundamentals $field $id "demand result" }
    }
    elseif ($null -ne $demandEnd -and $now -gt $demandEnd) {
        Add-Issue "WARNING" "demand_result_pending" "$id demand result is pending through $($demandResultDeadline.ToString('yyyy-MM-dd'))."
    }
    if ((Has-Value $fundamentals.lockupCommitmentRate) -and ([double]$fundamentals.lockupCommitmentRate -lt 0 -or [double]$fundamentals.lockupCommitmentRate -gt 1)) { Add-Issue "ERROR" "lockup_range" "$id lockupCommitmentRate must be a fraction from 0 to 1." }
    if ((Has-Value $fundamentals.floatRate) -and ([double]$fundamentals.floatRate -lt 0 -or [double]$fundamentals.floatRate -gt 1)) { Add-Issue "ERROR" "float_range" "$id floatRate must be a fraction from 0 to 1." }

    if ($null -ne $subscriptionEnd -and $now -gt $subscriptionEnd) {
        $latest = @($stock.snapshots | Sort-Object capturedAt | Select-Object -Last 1)
        if ($latest.Count -eq 0 -or -not (Has-Value $latest[0].aggregate.competitionRate)) { Add-Issue "ERROR" "retail_competition" "$id has no final aggregate retail competition after subscription close." }
        else {
            $brokers = @($latest[0].brokers)
            if ($brokers.Count -eq 0) { Add-Issue "ERROR" "broker_snapshot" "$id has no broker rows after subscription close." }
            foreach ($broker in $brokers) {
                if (-not (Has-Value $broker.applicationCount)) { Add-Issue "WARNING" "application_count" "$id/$($broker.name) has no application count." }
                if (-not (Has-Value $broker.equalAllocationShares) -and -not (Has-Value $broker.expectedEqualShares)) { Add-Issue "ERROR" "equal_allocation" "$id/$($broker.name) has no equal allocation value or estimate." }
            }
        }
    }

    $year = $subscriptionStart.Year
    if (-not $yearDocs.ContainsKey($year)) { Add-Issue "ERROR" "year_feed" "Missing yearly feed for $year." }
    else {
        $yearMatches = @($yearDocs[$year].stocks | Where-Object id -eq $id)
        if ($yearMatches.Count -ne 1) { Add-Issue "ERROR" "year_membership" "$id must occur once in yearly/$year.json; found $($yearMatches.Count)." }
        elseif ($yearMatches[0].path -ne "stocks/$id.json") { Add-Issue "ERROR" "year_path" "$id has an invalid yearly feed path '$($yearMatches[0].path)'." }
    }

    if ($manual.PSObject.Properties.Name -contains $id) {
        foreach ($property in $manual.$id.PSObject.Properties) {
            if ($fundamentals.PSObject.Properties.Name -contains $property.Name -and $fundamentals.($property.Name) -ne $property.Value) { Add-Issue "ERROR" "manual_mismatch" "$id generated $($property.Name) differs from manual fundamentals." }
        }
    }
}

$errors = @($Issues | Where-Object Level -eq "ERROR")
$warnings = @($Issues | Where-Object Level -eq "WARNING")
Write-Output "IPO monthly audit"
Write-Output "Range: $($rangeStart.ToString('yyyy-MM-dd')) to $($rangeEnd.ToString('yyyy-MM-dd')) (exclusive)"
Write-Output "Candidates: $($candidates.Count); included: $($includedCandidates.Count); excluded: $(@($candidates | Where-Object status -eq 'excluded').Count); unresolved: $(@($candidates | Where-Object status -eq 'unresolved').Count)"
Write-Output "Local events: $($events.Count); errors: $($errors.Count); warnings: $($warnings.Count)"
foreach ($issue in $Issues) { Write-Output "$($issue.Level) [$($issue.Code)] $($issue.Message)" }
if ($errors.Count -gt 0) { exit 1 }
