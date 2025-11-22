from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .service import IngestionService
from .defi_service import DefiIngestionService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

app = FastAPI(title="Crypto & DeFi Data Warehouse Service", version="0.2.0")
service = IngestionService()
defi_service = DefiIngestionService(top_n_protocols=100)


@app.on_event("startup")
async def on_startup() -> None:
    await service.start()
    # Start DeFi ingestion (fetches every 60 minutes - respects free tier)
    await defi_service.start(interval_minutes=60)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await service.stop()
    await defi_service.stop()


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok", "tracked_symbols": service.symbols}


@app.get("/stats")
async def stats() -> Dict[str, Any]:
    """Get performance statistics"""
    return service.get_stats()


@app.get("/latest/{symbol}")
async def latest_snapshot(symbol: str) -> JSONResponse:
    payload = service.latest_payload(symbol)
    if not payload:
        raise HTTPException(status_code=404, detail="Symbol not tracked or data unavailable")
    return JSONResponse(payload)


@app.get("/defi/protocols")
async def defi_protocols() -> JSONResponse:
    """Get latest DeFi protocols data"""
    try:
        protocols = await defi_service.defillama.get_top_protocols(100)
        return JSONResponse({"count": len(protocols), "protocols": protocols})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/defi/chains")
async def defi_chains() -> JSONResponse:
    """Get latest chain TVL data"""
    try:
        chains = await defi_service.defillama.get_chains_tvl()
        return JSONResponse({"chains": chains})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/defi/fetch")
async def trigger_defi_fetch() -> JSONResponse:
    """Manually trigger DeFi data fetch"""
    try:
        await defi_service.manual_fetch()
        return JSONResponse({"status": "success", "message": "DeFi fetch completed"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.server:app", host="0.0.0.0", port=8000, reload=False)
