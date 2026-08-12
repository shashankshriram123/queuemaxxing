"""Run the Queuemaxxing development server."""

import os

import uvicorn


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=os.getenv("QUEUEMAXXING_HOST", "127.0.0.1"),
        port=int(os.getenv("QUEUEMAXXING_PORT", "8000")),
        workers=1,
    )
