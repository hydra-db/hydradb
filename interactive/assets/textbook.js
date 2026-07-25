/* ==========================================================================
   TurboLay Interactive Textbook — runtime
   - Nested tooltips (Paradox-grand-strategy style: hover-open, lock, safe
     corridor, child chains, grace dismissal, Esc-to-close, cycle guard)
   - Theme toggle (persists in localStorage)
   - Reading-progress bar
   - Widget registry (chapters call TB.widget(id, mountFn))
   Concept definitions live in the page-loaded window.TB_GLOSSARY (glossary.js).
   ========================================================================== */
(function () {
  "use strict";

  /* ----------------------------- theme --------------------------------- */
  const root = document.documentElement;
  const saved = (function () { try { return localStorage.getItem("tb-theme"); } catch (e) { return null; } })();
  if (saved) root.setAttribute("data-theme", saved);

  function toggleTheme() {
    const cur = root.getAttribute("data-theme");
    const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    const isDark = cur ? cur === "dark" : prefersDark;
    const next = isDark ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try { localStorage.setItem("tb-theme", next); } catch (e) {}
    updateToggleLabel();
  }
  function updateToggleLabel() {
    const btn = document.querySelector(".tb-theme-toggle");
    if (!btn) return;
    const cur = root.getAttribute("data-theme");
    const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    const isDark = cur ? cur === "dark" : prefersDark;
    btn.innerHTML = isDark ? "☀︎ Light" : "☾ Dark";
  }

  /* ------------------------- reading progress -------------------------- */
  function initProgress() {
    const bar = document.querySelector(".tb-progress");
    if (!bar) return;
    const onScroll = function () {
      const h = document.documentElement;
      const scrolled = h.scrollTop;
      const max = h.scrollHeight - h.clientHeight;
      bar.style.width = (max > 0 ? (scrolled / max) * 100 : 0) + "%";
    };
    document.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
  }

  /* ---------------------------------------------------------------------
     Nested tooltip engine
     ------------------------------------------------------------------ */
  const HOVER_OPEN_MS = 280;   // hover before a tooltip opens
  const LOCK_MS = 420;         // after opening, becomes interactive
  const GRACE_MS = 260;        // leaving the whole chain before teardown
  const MAX_DEPTH = 5;
  const CORRIDOR = 26;         // px margin used to approximate the safe corridor

  const GL = function () { return window.TB_GLOSSARY || {}; };

  // active chain: array of { source, tip, slug, level, lockTimer, lockStart, lockRAF }
  let chain = [];
  let openTimer = null;
  let openTarget = null;
  let graceTimer = null;
  let mouse = { x: 0, y: 0 };

  document.addEventListener("mousemove", function (e) {
    mouse.x = e.clientX; mouse.y = e.clientY;
    if (chain.length) evaluatePresence();
  }, { passive: true });

  function pointNearRect(x, y, r, m) {
    return x >= r.left - m && x <= r.right + m && y >= r.top - m && y <= r.bottom + m;
  }

  // Is the pointer over the source of the deepest tooltip, over any tip in the
  // chain, or in the corridor between the cursor and the deepest tip? If none,
  // start the grace timer to tear down.
  function evaluatePresence() {
    if (!chain.length) return;
    let inside = false;
    for (let i = 0; i < chain.length; i++) {
      const c = chain[i];
      if (c.source && pointNearRect(mouse.x, mouse.y, c.source.getBoundingClientRect(), 6)) { inside = true; break; }
      if (c.tip && pointNearRect(mouse.x, mouse.y, c.tip.getBoundingClientRect(), CORRIDOR)) { inside = true; break; }
    }
    if (inside) {
      if (graceTimer) { clearTimeout(graceTimer); graceTimer = null; }
    } else if (!graceTimer) {
      graceTimer = setTimeout(function () { graceTimer = null; teardown(0); }, GRACE_MS);
    }
  }

  function positionTip(tip, anchorRect, level) {
    // measure
    tip.style.left = "-9999px"; tip.style.top = "0px";
    const tw = tip.offsetWidth, th = tip.offsetHeight;
    const vw = window.innerWidth, vh = window.innerHeight;
    let left, top;
    if (level === 0) {
      // bias below-right of the anchored word, away from it so it never occludes
      left = Math.min(anchorRect.left, vw - tw - 12);
      top = anchorRect.bottom + 10;
      if (top + th > vh - 8) top = Math.max(8, anchorRect.top - th - 10);
    } else {
      // child: to the right of parent tip, else to the left
      left = anchorRect.right + 12;
      if (left + tw > vw - 8) left = anchorRect.left - tw - 12;
      if (left < 8) left = Math.min(anchorRect.left, vw - tw - 12);
      top = anchorRect.top;
      if (top + th > vh - 8) top = Math.max(8, vh - th - 8);
    }
    left = Math.max(8, Math.min(left, vw - tw - 8));
    top = Math.max(8, Math.min(top, vh - th - 8));
    tip.style.left = left + "px";
    tip.style.top = top + "px";
  }

  function slugInChain(slug) {
    return chain.some(function (c) { return c.slug === slug; });
  }

  function buildTip(slug, def, level) {
    const tip = document.createElement("div");
    tip.className = "tb-tooltip";
    tip.setAttribute("data-level", level);
    const cycle = false;
    tip.innerHTML =
      '<div class="tt-title">' + escapeHtml(def.title || slug) +
      '<span class="tt-pin">hover to pin</span></div>' +
      '<div class="tt-body">' + def.html + "</div>" +
      '<div class="tt-lockbar"></div>';
    document.body.appendChild(tip);
    return tip;
  }

  function lockTip(entry) {
    if (!entry.tip) return;
    entry.tip.classList.add("locked");
    const pin = entry.tip.querySelector(".tt-pin");
    if (pin) pin.textContent = "pinned";
    // wire concept links inside this now-interactive tooltip
    wireConcepts(entry.tip, entry.level + 1);
  }

  function beginLock(entry) {
    entry.lockStart = performance.now();
    const bar = entry.tip.querySelector(".tt-lockbar");
    function step(now) {
      if (!entry.tip || entry.tip.classList.contains("locked")) return;
      const pct = Math.min(100, ((now - entry.lockStart) / LOCK_MS) * 100);
      if (bar) bar.style.width = pct + "%";
      if (pct >= 100) { lockTip(entry); return; }
      entry.lockRAF = requestAnimationFrame(step);
    }
    entry.lockRAF = requestAnimationFrame(step);
  }

  function openTooltip(source, slug, level) {
    const def = GL()[slug];
    if (!def) return;

    // depth cap: collapse oldest ancestor beyond root
    if (level >= MAX_DEPTH) { teardown(MAX_DEPTH - 1); level = MAX_DEPTH - 1; }
    // prune any siblings deeper than this level
    teardown(level);

    const cyclic = slugInChain(slug);
    const tip = buildTip(slug, def, level);
    if (cyclic) {
      tip.classList.add("locked");
      tip.style.borderStyle = "dashed";
      const b = tip.querySelector(".tt-body");
      const warn = document.createElement("p");
      warn.style.cssText = "color:var(--warn);font-size:0.8rem;margin-top:0.5rem;";
      warn.textContent = "↑ already open above — following this again would loop.";
      b.appendChild(warn);
    }
    positionTip(tip, source.getBoundingClientRect(), level);
    requestAnimationFrame(function () { tip.classList.add("visible"); });

    const entry = { source: source, tip: tip, slug: slug, level: level };
    chain.push(entry);
    if (!cyclic) beginLock(entry);
    if (graceTimer) { clearTimeout(graceTimer); graceTimer = null; }
  }

  // teardown everything at depth >= level
  function teardown(level) {
    for (let i = chain.length - 1; i >= 0; i--) {
      if (chain[i].level >= level) {
        const c = chain[i];
        if (c.lockRAF) cancelAnimationFrame(c.lockRAF);
        if (c.tip && c.tip.parentNode) {
          c.tip.classList.remove("visible");
          const t = c.tip;
          setTimeout(function () { if (t.parentNode) t.parentNode.removeChild(t); }, 130);
        }
        chain.splice(i, 1);
      }
    }
  }

  function wireConcepts(container, level) {
    const nodes = container.querySelectorAll(".concept[data-concept]");
    nodes.forEach(function (el) {
      if (el.__tbWired) return;
      el.__tbWired = true;
      el.setAttribute("tabindex", "0");
      el.setAttribute("role", "button");
      el.addEventListener("mouseenter", function () {
        if (openTimer) clearTimeout(openTimer);
        openTarget = el;
        openTimer = setTimeout(function () {
          openTooltip(el, el.getAttribute("data-concept"), level);
        }, HOVER_OPEN_MS);
      });
      el.addEventListener("mouseleave", function () {
        if (openTimer && openTarget === el) { clearTimeout(openTimer); openTimer = null; }
        if (chain.length) evaluatePresence();
      });
      // keyboard / touch: open immediately and lock
      el.addEventListener("click", function (e) {
        e.preventDefault();
        const already = chain.find(function (c) { return c.source === el; });
        if (already) { teardown(already.level); return; }
        openTooltip(el, el.getAttribute("data-concept"), level);
        const last = chain[chain.length - 1];
        if (last) lockTip(last);
      });
      el.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); el.click(); }
      });
    });
  }

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && chain.length) { teardown(0); }
  });
  window.addEventListener("scroll", function () { if (chain.length) teardown(0); }, { passive: true });

  function escapeHtml(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  /* --------------------------- widget registry ------------------------- */
  const widgets = {};
  function widget(id, mountFn) {
    widgets[id] = mountFn;
    const el = document.getElementById(id);
    if (el && !el.__tbMounted) { el.__tbMounted = true; try { mountFn(el); } catch (e) { console.error("widget", id, e); } }
  }

  /* ------------------------------ boot --------------------------------- */
  function boot() {
    const btn = document.querySelector(".tb-theme-toggle");
    if (btn) btn.addEventListener("click", toggleTheme);
    updateToggleLabel();
    if (window.matchMedia) {
      window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", updateToggleLabel);
    }
    initProgress();
    wireConcepts(document.body, 0);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();

  window.TB = { widget: widget, toggleTheme: toggleTheme };
})();
