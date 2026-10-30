import uvicorn

uvicorn.run("eclipse_hivemind.attack_coordinator.app:app", host="0.0.0.0", port=8090)
