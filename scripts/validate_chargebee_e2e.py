from __future__ import annotations

import asyncio
import json

from config.settings import DiscoverySettings
from core.orchestrator import run_signal_driven_lead_intelligence


async def main() -> None:
    settings = DiscoverySettings(
        regions=["India"],
        industries=["subscription billing"],
        target_min=1,
        target_max=1,
        batch_size=8,
        max_pages=1,
        request_timeout_seconds=4,
    )

    result = await asyncio.wait_for(
        run_signal_driven_lead_intelligence(settings=settings, target_min=1, target_max=1, validation_mode=True),
        timeout=120,
    )

    with open("output/chargebee_signal_engine.json", "w", encoding="utf-8") as handle:
        handle.write(json.dumps(result, indent=2, ensure_ascii=True))

    metadata = result.get("execution_metadata", {})
    print("saved output/chargebee_signal_engine.json")
    print("ranked", len(result.get("companies", [])))
    print("query_total", (metadata.get("query_plan") or {}).get("total_queries"))
    print("acquisition", json.dumps(metadata.get("acquisition", {}), ensure_ascii=True))
    print("dropoffs", json.dumps(metadata.get("dropoffs", {}), ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main())
