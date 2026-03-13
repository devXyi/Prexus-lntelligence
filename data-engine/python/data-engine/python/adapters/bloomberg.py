"""
Bloomberg BLPAPI Adapter
========================
REQUIREMENTS:
  - Active Bloomberg Terminal ($25k/yr) OR B-PIPE enterprise license
  - blpapi SDK: pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/ blpapi
  - Bloomberg Desktop/Server process running on localhost:8194

This is the REAL integration method — not a REST call to some wrapper.
Bloomberg's protocol is proprietary binary TCP, event-driven, async.
"""

import blpapi
import logging
from datetime import datetime, date
from typing import Optional
from dataclasses import dataclass

log = logging.getLogger(__name__)

BLOOMBERG_HOST = "localhost"
BLOOMBERG_PORT = 8194  # Standard BLPAPI port

# Bloomberg service namespaces
SVC_REF_DATA   = "//blp/refdata"    # Historical + reference data
SVC_MKT_DATA   = "//blp/mktdata"   # Real-time streaming
SVC_AUTH       = "//blp/apiauth"   # B-PIPE authentication


@dataclass
class MarketSnapshot:
    ticker:       str
    last_price:   float
    bid:          float
    ask:          float
    volume:       int
    timestamp:    datetime
    currency:     str
    exchange:     str


class BloombergSession:
    """
    Manages a persistent connection to Bloomberg API.
    Uses event-driven model — Bloomberg pushes data, we handle events.
    """

    def __init__(self, app_name: str = "PREXUS_METEORIUM"):
        self.app_name = app_name
        self.session: Optional[blpapi.Session] = None
        self._subscriptions: dict[str, blpapi.CorrelationId] = {}

    def connect(self) -> bool:
        """
        Establish TCP connection to Bloomberg server process.
        For Desktop API: connects to local Bloomberg Terminal process.
        For B-PIPE: connects to Bloomberg's network via dedicated line.
        """
        opts = blpapi.SessionOptions()
        opts.setServerHost(BLOOMBERG_HOST)
        opts.setServerPort(BLOOMBERG_PORT)

        # For B-PIPE enterprise (not needed for Desktop API):
        # opts.setAuthenticationOptions(
        #     f"AuthenticationMode=APPLICATION_ONLY;"
        #     f"ApplicationAuthenticationType=APPNAME_AND_KEY;"
        #     f"ApplicationName={self.app_name}"
        # )

        self.session = blpapi.Session(opts)

        if not self.session.start():
            log.error("Failed to connect to Bloomberg. Is Terminal/B-PIPE running?")
            return False

        log.info(f"Bloomberg session started → {BLOOMBERG_HOST}:{BLOOMBERG_PORT}")
        return True

    def authenticate_bpipe(self, uuid: str, ip_address: str) -> bool:
        """
        B-PIPE authentication — required for enterprise server deployments.
        Desktop API users skip this (Terminal handles auth itself).

        Bloomberg authenticates against their global entitlement servers.
        UUID is your Bloomberg Professional user ID.
        """
        if not self.session.openService(SVC_AUTH):
            log.error("Could not open auth service — check B-PIPE license")
            return False

        auth_svc = self.session.getService(SVC_AUTH)
        req = auth_svc.createAuthorizationRequest()
        req.set("uuid", uuid)
        req.set("ipAddress", ip_address)

        self.identity = self.session.createIdentity()
        self.session.sendAuthorizationRequest(req, self.identity)

        # Event loop — Bloomberg is async, we wait for response event
        while True:
            event = self.session.nextEvent(timeout_ms=5000)
            if event.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.REQUEST_STATUS):
                for msg in event:
                    if msg.messageType() == blpapi.Name("AuthorizationSuccess"):
                        log.info(f"Bloomberg auth OK for UUID={uuid}")
                        return True
                    elif msg.messageType() == blpapi.Name("AuthorizationFailure"):
                        log.error("Bloomberg auth FAILED — check UUID + entitlements")
                        return False

    def get_historical(
        self,
        tickers: list[str],
        fields:  list[str],
        start:   date,
        end:     date,
    ) -> dict[str, list[dict]]:
        """
        Historical data request (BDH equivalent).
        Returns OHLCV or any Bloomberg field for a date range.

        Example fields: ["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW", "VOLUME"]
        Example tickers: ["AAPL US Equity", "BTC Curncy", "CL1 Comdty"]

        Note: Tickers use Bloomberg's yellow-key notation — not just symbols.
        """
        if not self.session.openService(SVC_REF_DATA):
            raise RuntimeError("Cannot open refdata service")

        svc = self.session.getService(SVC_REF_DATA)
        req = svc.createRequest("HistoricalDataRequest")

        for t in tickers:
            req.getElement("securities").appendValue(t)
        for f in fields:
            req.getElement("fields").appendValue(f)

        req.set("startDate", start.strftime("%Y%m%d"))
        req.set("endDate",   end.strftime("%Y%m%d"))
        req.set("periodicitySelection", "DAILY")
        req.set("nonTradingDayFillOption", "ACTIVE_DAYS_ONLY")

        self.session.sendRequest(req)

        results: dict[str, list[dict]] = {}

        # Process response events until RESPONSE (final) event
        while True:
            event = self.session.nextEvent(timeout_ms=10_000)

            if event.eventType() in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
                for msg in event:
                    if msg.hasElement("securityData"):
                        sec_data = msg.getElement("securityData")
                        ticker   = sec_data.getElementAsString("security")
                        rows     = []
                        fd_array = sec_data.getElement("fieldData")

                        for i in range(fd_array.numValues()):
                            row = fd_array.getValueAsElement(i)
                            record = {"date": str(row.getElementAsDatetime("date").date())}
                            for f in fields:
                                if row.hasElement(f):
                                    record[f] = row.getElementAsFloat(f)
                            rows.append(record)

                        results[ticker] = rows

                if event.eventType() == blpapi.Event.RESPONSE:
                    break  # All data received

        return results

    def subscribe_realtime(
        self,
        tickers: list[str],
        fields:  list[str],
        callback,  # callable(ticker, data_dict)
    ) -> None:
        """
        Real-time streaming subscription (like Bloomberg LIVE screen).
        Bloomberg pushes updates as prices change — this is not polling.

        This is the core of how Bloomberg actually works:
        - You subscribe to tickers
        - Bloomberg's network pushes tick-level updates
        - You handle them in an event loop
        """
        if not self.session.openService(SVC_MKT_DATA):
            raise RuntimeError("Cannot open mktdata service")

        sub_list = blpapi.SubscriptionList()
        for ticker in tickers:
            corr_id = blpapi.CorrelationId(ticker)
            self._subscriptions[ticker] = corr_id
            sub_list.add(
                ticker,
                fields,
                correlationId=corr_id,
            )

        self.session.subscribe(sub_list)
        log.info(f"Subscribed to {len(tickers)} real-time feeds")

        # Event loop — runs indefinitely, pushing to callback
        while True:
            event = self.session.nextEvent(timeout_ms=500)

            if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                for msg in event:
                    ticker = str(msg.correlationIds()[0].value())
                    data   = {}
                    for f in fields:
                        if msg.hasElement(f):
                            data[f] = msg.getElement(f).getValueAsFloat()
                    data["_ts"] = datetime.utcnow().isoformat()
                    callback(ticker, data)

    def disconnect(self):
        if self.session:
            self.session.stop()
            log.info("Bloomberg session closed")


# ─── Convenience wrapper for Prexus use ──────────────────────────────────────

class BloombergDataAdapter:
    """
    High-level adapter used by Prexus data engine.
    Maps Bloomberg fields → Prexus asset risk schema.
    """

    # Climate-relevant Bloomberg tickers for physical + transition risk
    CARBON_PRICE    = "MOZ5 Comdty"        # EU ETS Carbon futures
    NAT_GAS         = "NG1 Comdty"         # Natural gas (transition proxy)
    CRUDE_OIL       = "CL1 Comdty"         # Brent crude
    VIX             = "VIX Index"           # Volatility index (systemic risk)
    MSCI_WORLD_ESG  = "M1WO0ESG Index"     # MSCI World ESG leaders

    def __init__(self):
        self.session = BloombergSession()

    def connect(self) -> bool:
        return self.session.connect()

    def get_carbon_risk_price(self) -> float:
        """EU carbon price — direct transition risk input."""
        result = self.session.get_historical(
            tickers=[self.CARBON_PRICE],
            fields=["PX_LAST"],
            start=date.today(),
            end=date.today(),
        )
        rows = result.get(self.CARBON_PRICE, [])
        return rows[-1]["PX_LAST"] if rows else 0.0

    def get_transition_risk_index(self, country_code: str) -> float:
        """
        Proxy: sovereign CDS spread as transition risk indicator.
        Higher spread = higher political/policy risk = higher transition risk.
        Normalized to 0.0–1.0 for Prexus schema.
        """
        cds_tickers = {
            "IND": "INDIAGOV CDS USD SR 5Y Corp",
            "CHN": "CHNGOV CDS USD SR 5Y Corp",
            "USA": "USGOV CDS USD SR 5Y Corp",
            "GBR": "UKGOV CDS USD SR 5Y Corp",
        }
        ticker = cds_tickers.get(country_code, "USGOV CDS USD SR 5Y Corp")
        result = self.session.get_historical(
            tickers=[ticker],
            fields=["PX_LAST"],
            start=date.today(),
            end=date.today(),
        )
        rows = result.get(ticker, [])
        if not rows:
            return 0.5  # Default fallback

        spread_bps = rows[-1]["PX_LAST"]
        # 0 bps = no risk, 500+ bps = extreme risk. Normalize to [0, 1]
        return min(spread_bps / 500.0, 1.0)
