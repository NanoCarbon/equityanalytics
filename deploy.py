from dotenv import load_dotenv
load_dotenv()

from prefect import serve
from ingestion.pipeline import equity_pipeline, macro_pipeline, backfill_pipeline
from ingestion.pipeline_fundamentals import (
    fundamentals_test_pipeline,
    fundamentals_backfill_pipeline,
    fundamentals_pipeline,
    valuation_pipeline,
)

if __name__ == "__main__":
    equity_deploy = equity_pipeline.to_deployment(
        name="daily-equity-ingestion",
        cron="0 9 * * 1-5"
    )

    macro_deploy = macro_pipeline.to_deployment(
        name="daily-macro-ingestion",
        cron="0 9 * * 1-5"
    )

    backfill_deploy = backfill_pipeline.to_deployment(
        name="historical-backfill",
        parameters={
            "start_date": "2010-01-01",
            "batch_size": 50,
            "batch_delay_seconds": 30
        }
    )

    # ── Fundamentals ──────────────────────────────────────────────────────

    fundamentals_test_deploy = fundamentals_test_pipeline.to_deployment(
        name="test-fundamentals",
        # No schedule — manual trigger only
    )

    fundamentals_backfill_deploy = fundamentals_backfill_pipeline.to_deployment(
        name="backfill-fundamentals",
        parameters={
            "batch_size": 50,
            "batch_delay_seconds": 30,
            "delay_seconds": 2.0,
        }
    )

    fundamentals_weekly_deploy = fundamentals_pipeline.to_deployment(
        name="weekly-fundamentals-ingestion",
        cron="0 10 * * 6"  # Saturday 10am
    )

    valuation_daily_deploy = valuation_pipeline.to_deployment(
        name="daily-valuation-snapshot",
        cron="0 9 * * 1-5"  # Weekdays 9am alongside equity pipeline
    )

    serve(
        equity_deploy,
        macro_deploy,
        backfill_deploy,
        fundamentals_test_deploy,
        fundamentals_backfill_deploy,
        fundamentals_weekly_deploy,
        valuation_daily_deploy,
    )
