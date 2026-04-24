# ECCO Sea Ice Velocity — Particle Demo

An animated particle visualization of Arctic sea ice motion using the
[ECCO L4 Sea Ice Velocity](https://cmr.earthdata.nasa.gov/search/concepts/C1990404790-POCLOUD)
dataset served by TiTiler-CMR.

**Stack:** [MapLibre GL JS](https://maplibre.org/) (free, no API token required) +
[Vite](https://vitejs.dev/) dev server + custom 2D canvas particle system.

## How it works

1. The Vite dev server proxies `/titiler → http://localhost:8081` to avoid CORS.
2. On load (or when the user picks a year/month), the app fetches a single full-world
   PNG from the TiTiler-CMR `/xarray/bbox` endpoint with both velocity variables:
   ```
   /xarray/bbox/-180,-90,180,90.png
     ?collection_concept_id=C1990404790-POCLOUD
     &variables=SIeice
     &variables=SInice
     &temporal=1992-01-16T18:00:00Z
     &rescale=-0.4,0.4
   ```
   The PNG encodes: **R = SIeice (eastward)**, **G = SInice (northward)**,
   **B = 255 means valid data**.
3. The pixel array is decoded on the CPU and sampled each animation frame.
4. 4 000 particles are simulated on a transparent `<canvas>` overlay; each particle
   moves proportionally to the local U/V velocity and is coloured by speed magnitude
   (blue → white gradient).

## Prerequisites

- Docker with TiTiler-CMR running on port 8081:
  ```bash
  docker-compose up -d
  ```
- Node.js ≥ 18

## Run

```bash
cd docs/api/particle_demo
npm install
npm run dev
```

Open `http://localhost:5173` in your browser.

Use the **Year / Month** selectors and **Load** button to browse the 1992–2018
monthly time series.
