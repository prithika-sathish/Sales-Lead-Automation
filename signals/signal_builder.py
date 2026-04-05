from __future__ import annotations


class SignalBuilder:
    FUNDING_TOKENS = ["raised", "funding", "series a", "series b", "investment"]
    HIRING_TOKENS = ["hiring", "sdr", "account executive", "growth manager", "sales"]
    PRODUCT_TOKENS = ["api", "platform", "saas", "b2b"]

    def build(self, text: str, *, regions: list[str]) -> dict[str, bool]:
        lowered = str(text or "").lower()
        return {
            "hiring": any(token in lowered for token in self.HIRING_TOKENS),
            "funding": any(token in lowered for token in self.FUNDING_TOKENS),
            "b2b": any(token in lowered for token in self.PRODUCT_TOKENS),
            "region_match": any(str(region or "").lower() in lowered for region in regions),
        }
