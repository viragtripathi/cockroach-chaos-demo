// Hover tooltips for any element with a data-tip attribute (stage and presenter).
// Works inside a CSS-scaled layer (the stage) and survives re-renders: the tooltip re-anchors to whatever
// target is under the pointer whenever the page changes.

export function tip(node, text) {
  if (node && text) node.dataset.tip = text;
  return node;
}

export function initTips({ layer = document.body, scale = () => 1, delay = 250 } = {}) {
  const box = document.createElement('div');
  box.className = 'tooltip';
  box.setAttribute('role', 'tooltip');
  box.hidden = true;
  layer.append(box);

  let anchor = null;
  let pending = null;
  let mouse = null;

  const place = () => {
    const s = scale() || 1;
    const c = layer.getBoundingClientRect();
    const a = anchor.getBoundingClientRect();
    const width = c.width / s;
    const w = box.offsetWidth, h = box.offsetHeight;
    let x = (a.left + a.width / 2 - c.left) / s - w / 2;
    let y = (a.top - c.top) / s - h - 12;
    if (y < 8) y = (a.bottom - c.top) / s + 12;  // no room above: show it below
    box.style.left = `${Math.max(8, Math.min(width - w - 8, x))}px`;
    box.style.top = `${y}px`;
  };

  const show = (target) => {
    anchor = target;
    box.textContent = target.dataset.tip;
    box.hidden = false;
    place();
  };

  const hide = () => {
    clearTimeout(pending);
    box.hidden = true;
    anchor = null;
  };

  document.addEventListener('mouseover', (e) => {
    const target = e.target.closest && e.target.closest('[data-tip]');
    if (!target) { if (anchor || pending) hide(); return; }
    if (target === anchor) return;
    clearTimeout(pending);
    if (anchor) show(target);  // already showing one: switch without the delay
    else pending = setTimeout(() => show(target), delay);
  });
  document.addEventListener('mousemove', (e) => { mouse = [e.clientX, e.clientY]; }, { passive: true });
  document.addEventListener('scroll', hide, true);
  document.addEventListener('pointerdown', hide, true);

  // Re-render replaced the target? Re-anchor to the element now under the pointer, with fresh text.
  new MutationObserver(() => {
    if (!anchor || !mouse || box.contains(anchor)) return;
    if (document.contains(anchor)) { if (box.textContent !== anchor.dataset.tip) show(anchor); return; }
    const under = document.elementFromPoint(mouse[0], mouse[1]);
    const next = under && under.closest('[data-tip]');
    if (next) show(next); else hide();
  }).observe(document.body, { subtree: true, childList: true, attributes: true, attributeFilter: ['data-tip'] });

  return { hide };
}
