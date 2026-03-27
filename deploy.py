from dotenv import load_dotenv
load_dotenv()

from ingestion.pipeline import equity_pipeline

if __name__ == "__main__":
    equity_pipeline.serve(
        name="daily-equity-ingestion",
        cron="0 9 * * 1-5"
    )