import 'maplibre-gl/dist/maplibre-gl.css';
import maplibregl from 'maplibre-gl';

// ── Constants ─────────────────────────────────────────────────────────────────
const MAP_STYLE   = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';
const COLLECTION  = 'C1990404790-POCLOUD';
const DATA_BOUNDS = { minLon: -180, maxLon: 180, minLat: -90, maxLat: 90 };

const NUM_PARTICLES = 4000;
const MAX_AGE       = 50;
const SPEED_FACTOR  = 4.0;   // degrees/step — boosts tiny sea-ice speeds to visible movement
const FADE_OPACITY  = 0.93;
const DROP_RATE     = 0.004;

// ── Populate year dropdown (1992 – 2018) ──────────────────────────────────────
const yearSel = document.getElementById('sel-year');
for (let y = 1992; y <= 2018; y++) {
  const opt = document.createElement('option');
  opt.value = y; opt.textContent = y;
  yearSel.appendChild(opt);
}
// Start on Jan 1992
yearSel.value = '1992';

// Disable month 2-12 when year = 2018 (dataset ends Jan 2018)
function updateMonthOptions() {
  const y = parseInt(yearSel.value);
  const monthSel = document.getElementById('sel-month');
  [...monthSel.options].forEach((o, i) => {
    o.disabled = y === 2018 && i > 0;
  });
  if (y === 2018) monthSel.value = '01';
}
yearSel.addEventListener('change', updateMonthOptions);
updateMonthOptions();

// ── Status helper ─────────────────────────────────────────────────────────────
function setStatus(msg, isError = false) {
  const el = document.getElementById('status');
  el.textContent = msg;
  el.className = isError ? 'error' : '';
}

// ── Build the TiTiler URL for a given year/month ──────────────────────────────
function buildImageUrl(year, month) {
  const temporal = `${year}-${month}-16T18:00:00Z`;
  return '/titiler/xarray/bbox/-180.0,-90.0,180.0,90.0.png'
    + `?collection_concept_id=${COLLECTION}`
    + '&variables=SIeice&variables=SInice'
    + `&temporal=${encodeURIComponent(temporal)}`
    + '&rescale=-0.4,0.4';
}

// ── Fetch PNG → RGBA pixel array ──────────────────────────────────────────────
async function fetchVelocityField(url, timeoutMs = 90000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let response;
  try {
    response = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(err.name === 'AbortError' ? `Timed out (${timeoutMs / 1000}s)` : err.message);
  }
  clearTimeout(timer);
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(`HTTP ${response.status}: ${body.slice(0, 120)}`);
  }
  const blob   = await response.blob();
  const bitmap = await createImageBitmap(blob);
  const c = document.createElement('canvas');
  c.width = bitmap.width; c.height = bitmap.height;
  c.getContext('2d').drawImage(bitmap, 0, 0);
  const { data, width, height } = c.getContext('2d').getImageData(0, 0, bitmap.width, bitmap.height);
  return { pixels: data, width, height };
}

// ── Sample U/V from pixel array at a lon/lat ──────────────────────────────────
function sampleVelocity(field, lon, lat) {
  const { pixels, width, height } = field;
  if (lon < DATA_BOUNDS.minLon || lon > DATA_BOUNDS.maxLon) return null;
  if (lat < DATA_BOUNDS.minLat || lat > DATA_BOUNDS.maxLat) return null;

  const px  = Math.floor((lon - DATA_BOUNDS.minLon) / (DATA_BOUNDS.maxLon - DATA_BOUNDS.minLon) * width);
  const py  = Math.floor((DATA_BOUNDS.maxLat - lat)  / (DATA_BOUNDS.maxLat - DATA_BOUNDS.minLat) * height);
  const idx = (Math.min(py, height - 1) * width + Math.min(px, width - 1)) * 4;

  if (pixels[idx + 2] < 128) return null; // B channel: 255 = valid, 0 = nodata

  // Unscale 0-255 → -0.4 to +0.4 m/s
  const u = (pixels[idx]     / 255) * 0.8 - 0.4;  // R = SIeice (eastward)
  const v = (pixels[idx + 1] / 255) * 0.8 - 0.4;  // G = SInice (northward)
  return { u, v };
}

// ── Particle state ─────────────────────────────────────────────────────────────
const particles = new Float32Array(NUM_PARTICLES * 3); // [lon, lat, age]

function initParticle(i) {
  particles[i * 3]     = -180 + Math.random() * 360;
  particles[i * 3 + 1] = -85  + Math.random() * 170;
  particles[i * 3 + 2] = Math.floor(Math.random() * MAX_AGE);
}
for (let i = 0; i < NUM_PARTICLES; i++) initParticle(i);

// ── Canvas setup ──────────────────────────────────────────────────────────────
const canvas = document.getElementById('particle-canvas');
const ctx    = canvas.getContext('2d');

function resizeCanvas() {
  canvas.width  = window.innerWidth;
  canvas.height = window.innerHeight;
}
resizeCanvas();
window.addEventListener('resize', resizeCanvas);

// ── MapLibre map ──────────────────────────────────────────────────────────────
const map = new maplibregl.Map({
  container: 'map-container',
  style: MAP_STYLE,
  center: [-20, 75],
  zoom: 2.4
});

// Clear trails when map moves so particles re-project correctly
map.on('movestart', () => ctx.clearRect(0, 0, canvas.width, canvas.height));

// ── Animation loop ────────────────────────────────────────────────────────────
let field     = null;
let animating = false;
let rafId     = null;

function stopAnimation() {
  animating = false;
  if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
  ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function startAnimation() {
  animating = true;
  function frame() {
    if (!animating || !field) return;
    rafId = requestAnimationFrame(frame);

    const W = canvas.width, H = canvas.height;

    // Fade existing trail
    ctx.globalCompositeOperation = 'destination-in';
    ctx.globalAlpha = FADE_OPACITY;
    ctx.fillRect(0, 0, W, H);
    ctx.globalCompositeOperation = 'source-over';
    ctx.globalAlpha = 1;

    ctx.strokeStyle = 'rgba(97,173,234,0.75)';
    ctx.lineWidth   = 1.5;
    ctx.lineCap     = 'round';

    for (let i = 0; i < NUM_PARTICLES; i++) {
      const lon = particles[i * 3];
      const lat = particles[i * 3 + 1];
      let   age = particles[i * 3 + 2] + 1;

      if (age > MAX_AGE || Math.random() < DROP_RATE) { initParticle(i); continue; }

      const vel = sampleVelocity(field, lon, lat);
      if (!vel) { initParticle(i); continue; }

      const newLon = lon + vel.u * SPEED_FACTOR;
      const newLat = lat + vel.v * SPEED_FACTOR;

      const p1 = map.project([lon, lat]);
      const p2 = map.project([newLon, newLat]);

      if (p1.x > -10 && p1.x < W + 10 && p1.y > -10 && p1.y < H + 10) {
        ctx.beginPath();
        ctx.moveTo(p1.x, p1.y);
        ctx.lineTo(p2.x, p2.y);
        ctx.stroke();
      }

      particles[i * 3]     = newLon;
      particles[i * 3 + 1] = newLat;
      particles[i * 3 + 2] = age;
    }
  }
  rafId = requestAnimationFrame(frame);
}

// ── Load a time step ──────────────────────────────────────────────────────────
async function loadTimeStep(year, month) {
  stopAnimation();

  const btn = document.getElementById('load-btn');
  btn.disabled = true;
  setStatus(`Loading ${year}-${month}… (first load ~15s)`);

  try {
    field = await fetchVelocityField(buildImageUrl(year, month));
    setStatus(`${year}-${month} — ${field.width}×${field.height} px`);
    startAnimation();
  } catch (err) {
    setStatus('Error: ' + err.message, true);
  } finally {
    btn.disabled = false;
  }
}

// ── Wire up Load button ───────────────────────────────────────────────────────
document.getElementById('load-btn').addEventListener('click', () => {
  const year  = document.getElementById('sel-year').value;
  const month = document.getElementById('sel-month').value;
  loadTimeStep(year, month);
});

// ── Initial load on map ready ─────────────────────────────────────────────────
map.on('load', () => loadTimeStep('1992', '01'));
