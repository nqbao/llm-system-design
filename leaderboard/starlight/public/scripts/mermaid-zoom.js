const zoomOverlayId = 'mermaid-zoom-overlay';

function createOverlay() {
  const existing = document.getElementById(zoomOverlayId);
  if (existing) existing.remove();

  const overlay = document.createElement('div');
  overlay.id = zoomOverlayId;
  overlay.innerHTML = `
    <div class="mermaid-zoom-backdrop"></div>
    <div class="mermaid-zoom-container">
      <button class="mermaid-zoom-close" aria-label="Close">&times;</button>
      <div class="mermaid-zoom-content"></div>
    </div>
  `;
  document.body.appendChild(overlay);

  overlay.querySelector('.mermaid-zoom-backdrop').addEventListener('click', closeZoom);
  overlay.querySelector('.mermaid-zoom-close').addEventListener('click', closeZoom);
  document.addEventListener('keydown', onKeyDown);

  return overlay;
}

function onKeyDown(e) {
  if (e.key === 'Escape') closeZoom();
}

function closeZoom() {
  const overlay = document.getElementById(zoomOverlayId);
  if (overlay) {
    overlay.classList.remove('open');
    setTimeout(() => overlay.remove(), 300);
  }
  document.removeEventListener('keydown', onKeyDown);
}

function openZoom(svgEl) {
  const overlay = createOverlay();
  const content = overlay.querySelector('.mermaid-zoom-content');
  const clone = svgEl.cloneNode(true);
  clone.style.maxWidth = '100%';
  clone.style.maxHeight = '90vh';
  content.appendChild(clone);
  requestAnimationFrame(() => overlay.classList.add('open'));
}

function setupMermaidClick() {
  document.querySelectorAll('.mermaid').forEach(el => {
    if (el.dataset.zoomAttached) return;
    el.dataset.zoomAttached = 'true';
    el.style.cursor = 'pointer';
    el.addEventListener('click', (e) => {
      if (e.target.closest('a, button')) return;
      const svg = el.querySelector('svg');
      if (svg) openZoom(svg);
    });
  });
}

function init() {
  setupMermaidClick();
  const observer = new MutationObserver(() => setupMermaidClick());
  observer.observe(document.body, { childList: true, subtree: true });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}

document.addEventListener('astro:after-swap', () => {
  setTimeout(setupMermaidClick, 100);
});
