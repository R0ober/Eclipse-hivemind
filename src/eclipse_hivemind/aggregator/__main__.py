"""Run the aggregator API with Uvicorn."""

import uvicorn


if __name__ == "__main__":
    uvicorn.run(
        "eclipse_hivemind.aggregator.app:app",
        host="0.0.0.0",
        port=8080,
    )
