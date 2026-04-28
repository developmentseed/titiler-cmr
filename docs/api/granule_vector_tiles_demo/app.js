const TITILER = `${window.location.origin}/titiler`;

const statusEl  = document.getElementById('status');
const statVis   = document.getElementById('stat-visible');
const statTiles = document.getElementById('stat-tiles');
const statZoom  = document.getElementById('stat-zoom');

let tilesFetched = 0;

const map = new maplibregl.Map({
  container: 'map',
  transformRequest: (url, resourceType) => {
    if (resourceType === 'Tile' && url.includes('/granules')) {
      tilesFetched++;
      statTiles.textContent = tilesFetched;
    }
    return { url };
  },
  style: {
    version: 8,
    sources: {
      carto: {
        type: 'raster',
        tiles: ['https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'],
        tileSize: 256,
        attribution: '© OpenStreetMap contributors © CARTO',
      },
    },
    layers: [{ id: 'carto', type: 'raster', source: 'carto' }],
  },
  center: [0, 72],
  zoom: 2,
});

// ── Zoom controls ──────────────────────────────────────────────────────────
document.getElementById('btn-zoom-in').addEventListener('click', () => map.zoomIn());
document.getElementById('btn-zoom-out').addEventListener('click', () => map.zoomOut());

map.on('zoom', () => {
  statZoom.textContent = map.getZoom().toFixed(1);
});

// ── Auto-fill temporal when collection changes ─────────────────────────────
document.getElementById('sel-collection').addEventListener('change', (e) => {
  const opt = e.target.selectedOptions[0];
  document.getElementById('inp-temporal').value = opt.dataset.temporal || '';
});

// ── Tile URL builder ───────────────────────────────────────────────────────
function buildTileUrl(collectionId, temporal) {
  const params = new URLSearchParams({ collection_concept_id: collectionId, temporal, f: 'mvt' });
  return `${TITILER}/tiles/WebMercatorQuad/{z}/{x}/{y}/granules?${params}`;
}

// ── Load granules ──────────────────────────────────────────────────────────
function loadGranules() {
  const collectionId = document.getElementById('sel-collection').value;
  const temporal     = document.getElementById('inp-temporal').value.trim();
  const btn          = document.getElementById('load-btn');

  if (!temporal) { statusEl.textContent = 'Enter a temporal value.'; return; }

  btn.disabled = true;
  btn.textContent = 'Loading…';
  tilesFetched = 0;
  statTiles.textContent = '0';
  statVis.textContent = '—';
  statusEl.textContent = '⏳ Requesting tiles from NASA CMR…';

  ['granules-fill', 'granules-outline'].forEach(id => { if (map.getLayer(id)) map.removeLayer(id); });
  if (map.getSource('granules')) map.removeSource('granules');

  map.addSource('granules', {
    type: 'vector',
    tiles: [buildTileUrl(collectionId, temporal)],
    minzoom: 0,
    maxzoom: 8,
  });

  map.addLayer({
    id: 'granules-fill',
    type: 'fill',
    source: 'granules',
    'source-layer': 'granules',
    paint: { 'fill-color': '#38bdf8', 'fill-opacity': 0.15 },
  });

  map.addLayer({
    id: 'granules-outline',
    type: 'line',
    source: 'granules',
    'source-layer': 'granules',
    paint: { 'line-color': '#38bdf8', 'line-width': 1.5 },
  });

  map.on('dataloading', () => { statusEl.textContent = '⏳ Fetching tiles from NASA CMR…'; });


  map.on('idle', () => {
    const features = map.queryRenderedFeatures({ layers: ['granules-fill'] });
    statVis.textContent = features.length > 0 ? features.length : '0';
    statusEl.textContent = features.length > 0
      ? `✅ ${features.length} granule footprint(s) in view — click one for details`
      : '✅ Tiles loaded — pan to a region with data';
    btn.disabled = false;
    btn.textContent = 'Load Granules';
  });

  // ── Popup on click ─────────────────────────────────────────────────────
  map.off('click', 'granules-fill');
  map.on('click', 'granules-fill', (e) => {
    const props = e.features[0].properties;
    const id    = props.id || '—';
    const cmrUrl = `https://cmr.earthdata.nasa.gov/search/concepts/${id}`;
    new maplibregl.Popup({ closeButton: true, maxWidth: '260px' })
      .setLngLat(e.lngLat)
      .setHTML(`
        <div class="popup-id">Granule ID: ${id}</div>
        <div class="popup-row"><span>Temporal</span><span>${props.temporal || '—'}</span></div>
        <a class="popup-link" href="${cmrUrl}" target="_blank">🔗 View granule metadata →</a>
      `)
      .addTo(map);
  });

  map.on('mouseenter', 'granules-fill', () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'granules-fill', () => { map.getCanvas().style.cursor = ''; });
}

document.getElementById('load-btn').addEventListener('click', loadGranules);

map.on('load', loadGranules);
