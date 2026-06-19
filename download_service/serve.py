"""Entry point for `meetupmap-download` CLI script."""
import uvicorn


def main() -> None:
    uvicorn.run(
        "download_service.main:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )


if __name__ == "__main__":
    main()
