from dotenv import load_dotenv
load_dotenv()

from prefect import serve
from ingestion.pipeline import equity_pipeline, macro_pipeline

if __name__ == "__main__":
    equity_deploy = equity_pipeline.to_deployment(
        name="daily-equity-ingestion",
        cron="0 9 * * 1-5"
    )

    macro_deploy = macro_pipeline.to_deployment(
        name="daily-macro-ingestion",
        cron="0 9 * * 1-5"
    )

    serve(equity_deploy, macro_deploy)