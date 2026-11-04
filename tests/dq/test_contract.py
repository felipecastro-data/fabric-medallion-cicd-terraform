from datetime import date

from pyspark.sql import functions as F

from divvy_medallion import contracts
from divvy_medallion.dq.rules import run_checks
from divvy_medallion.dq.suite import contract_period_checks, contract_table_checks
from divvy_medallion.silver.trips import MEMBER_TYPES, RIDEABLE_TYPES, SILVER_COLUMNS

TODAY = date(2026, 10, 22)


def outcomes(results):
    return {r.check: r.outcome for r in results}


def test_contract_ships_in_the_package_and_matches_the_silver_code():
    contract = contracts.load()

    assert contract.dataset == "lh_silver.dbo.trips" and contract.owner
    assert [c.name for c in contract.columns] == list(SILVER_COLUMNS)  # the contract documents every column
    assert contract.primary_key == ("_period", "ride_id")
    values = {c.name: c.accepted_values for c in contract.columns if c.accepted_values}
    assert values == {"rideable_type": RIDEABLE_TYPES, "member_type": MEMBER_TYPES}


def test_silver_trips_honours_its_contract(medallion):
    contract = contracts.load()
    results = run_checks(
        [
            *contract_table_checks(medallion.silver_trips, contract, TODAY),
            *contract_period_checks(medallion.silver_trips, contract, "202609"),
        ]  # fmt: skip
    )
    assert set(outcomes(results).values()) == {"passed"}
    assert len(results) == 6  # schema, freshness, required, primary key, 2 accepted-value sets


def test_schema_drift_breaks_the_contract(medallion):
    drifted = medallion.silver_trips.withColumn("start_hour", F.col("start_hour").cast("string")).withColumn(
        "surprise", F.lit(1)
    )
    result = run_checks(contract_table_checks(drifted, contracts.load(), TODAY))[0]

    assert result.outcome == "failed"
    assert "unexpected=['surprise']" in result.details and "start_hour: string != int" in result.details


def test_null_required_column_duplicate_key_and_unknown_value_fail(medallion):
    first = medallion.silver_trips.limit(1)
    broken = (
        medallion.silver_trips.unionByName(first)
        .unionByName(first.withColumn("ride_id", F.lit("NEW")).withColumn("member_type", F.lit("staff")))
        .unionByName(first.withColumn("ride_id", F.lit("NULLS")).withColumn("_batch_id", F.lit(None).cast("string")))
    )

    results = outcomes(run_checks(contract_period_checks(broken, contracts.load(), "202609")))
    assert results["contract.trips.required"] == "failed"
    assert results["contract.trips.primary_key"] == "failed"
    assert results["contract.trips.member_type"] == "failed"
    assert results["contract.trips.rideable_type"] == "passed"


def test_stale_data_only_warns(medallion):
    results = outcomes(run_checks(contract_table_checks(medallion.silver_trips, contracts.load(), date(2027, 6, 1))))
    assert results["contract.trips.freshness"] == "warning"
