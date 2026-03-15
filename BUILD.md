# Prexus Intelligence — Build & Run Guide

## Repository Structure After This Update

```
Prexus-Intelligence/
├── .env.example                    ← copy to .env, fill keys
├── render.yaml                     ← Render.com deployment
│
├── backend/
│   ├── go.mod
│   └── apps/api-gateway/
│       ├── main.go                 ← server entry point
│       ├── auth.go                 ← register / login / JWT
│       ├── db.go                   ← PostgreSQL + asset CRUD
│       ├── risk.go                 ← proxy to Python engine
│       └── claude.go               ← (your existing AI proxy)
│
├── data-engine/
│   ├── rust/
│   │   ├── Cargo.toml
│   │   └── src/lib.rs              ← Monte Carlo engine (PyO3)
│   └── python/
│       ├── api.py                  ← FastAPI main service
│       ├── requirements.txt
│       ├── adapters/
│       │   ├── base.py             ← adapter interface
│       │   ├── openmeteo.py        ← weather + ERA5 baseline
│       │   ├── firms_viirs.py      ← NASA fire detections
│       │   ├── carbon_monitor.py   ← CO2 emissions
│       │   └── era5.py             ← full ERA5 via CDS (optional)
│       └── core/
│           └── risk_engine.py      ← scoring + Monte Carlo bridge
│
└── frontend/
    └── index.html                  ← Meteorium UI (fixed)
```

---

## Step 1 — Compile the Rust Engine

The Rust core gives 60× speedup for Monte Carlo. Without it,
Python fallback runs automatically (slower but functional).

```bash
# Install Rust if needed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install maturin (PyO3 build tool)
pip install maturin

# Build and install into Python environment
cd data-engine/rust
maturin develop --release

# Verify
python3 -c "import meteorium_engine; print('✓ Rust engine loaded')"
```

After this, GitHub will show Rust as a language in the repo.

---

## Step 2 — Run Python Data Engine (local)

```bash
cd data-engine/python

# Create virtualenv
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy env file
cp ../../.env.example ../../.env
# Fill in at minimum: NASA_FIRMS_KEY (free, 2 min to get)

# Start engine
python api.py
# → http://localhost:8001
# → http://localhost:8001/docs  (Swagger UI)
# → http://localhost:8001/risk/health
```

---

## Step 3 — Run Go API Gateway (local)

```bash
cd backend

# Download dependencies
go mod tidy

# Run
go run ./apps/api-gateway/...
# → http://localhost:8080
# → http://localhost:8080/health
```

---

## Step 4 — Get Free API Keys

| Source         | Key Required | Where                                          | Time    |
|----------------|-------------|------------------------------------------------|---------|
| Open-Meteo     | No          | —                                              | —       |
| Carbon Monitor | No          | —                                              | —       |
| NASA FIRMS     | Yes (free)  | firms.modaps.eosdis.nasa.gov/api/              | 2 min   |
| Copernicus CDS | Yes (free)  | cds.climate.copernicus.eu/user/register        | 5 min   |
| Gemini AI      | Yes (free)  | aistudio.google.com/app/apikey                 | 1 min   |

Without keys: Open-Meteo + Carbon Monitor static fallbacks run automatically.
Meteorium works, just with reduced data richness.

---

## Step 5 — Deploy to Render

The `render.yaml` in the root configures everything automatically:

1. Push to GitHub
2. Go to render.com → New → Blueprint
3. Connect your `Prexus-Intelligence` repo
4. Render reads `render.yaml` and creates both services + database
5. Add secret env vars in Render dashboard (NASA_FIRMS_KEY, etc.)
6. Deploy

Both services auto-deploy on every push to `main`.

---

## API Endpoints

### Public
```
GET  /health                    → service health
POST /register                  → create account
POST /login                     → get JWT token
```

### Protected (requires Bearer token)
```
GET    /assets                  → list your assets
POST   /assets                  → create asset
PUT    /assets/:id              → update asset
DELETE /assets/:id              → delete asset

POST   /risk/asset              → score single asset (live telemetry)
POST   /risk/portfolio          → score portfolio (correlated MC)
POST   /risk/stress-test        → SSP1/2/3/5 scenario stress test
GET    /risk/health             → data engine + adapter status

POST   /analyze                 → AI analysis (Gemini/Claude/GPT-4o)
POST   /chat                    → AI chat (multi-turn)
```

---

## Why Git Shows Only 3 Languages

Rust shows up in GitHub's language bar only when:
1. `.rs` files exist (not `.rs.txt`)
2. The `Cargo.toml` is at the correct path
3. The files contain actual Rust code (not just comments)

After this update + `maturin develop`, GitHub will detect:
- HTML   ~55%  (frontend)
- Python ~22%  (data engine)
- Go     ~14%  (API gateway)
- Rust   ~9%   (Monte Carlo engine)
