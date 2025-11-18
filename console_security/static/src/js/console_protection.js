/** @odoo-module **/

// Minimal, safe console protection: only block alert(), document.cookie writes, and javascript: navigations.
// Also sanitize Odoo error dialogs for non-support users (friendly message + reference id).
(function () {
  'use strict';

  let config = {
    enabled: false,
    mode: 'off', // off | logging | protection
    debugKey: '',
    debugMode: false,
    isSupport: false,
  };

  async function loadConfig() {
    try {
      const response = await fetch('/web/console_security/params', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', method: 'call', id: Math.floor(Math.random() * 1e6) }),
      });
      if (!response.ok) throw new Error('Failed to fetch config');
      const data = await response.json();
      if (data && data.result) {
        config.enabled = !!data.result.enabled;
        config.mode = data.result.mode || 'off';
        config.debugKey = data.result.debug_key || '';
        config.isSupport = !!data.result.is_support;
      }
    } catch (e) {
      // Fail open to avoid breaking UI
      config.enabled = false;
      config.isSupport = true; // default to show errors if uncertain
    }
  }

  // Debug bypass for developers
  window.enableDebug = function (key) {
    if (key && key === config.debugKey) {
      config.debugMode = true;
      console.log('[Console Security] Debug mode enabled');
      return true;
    }
    console.error('[Console Security] Invalid debug key');
    return false;
  };

  function isConsoleExecution() {
    if (config.debugMode) return false;
    try {
      const stack = new Error().stack || '';
      // Whitelist known Odoo/Owl assets to avoid false positives
      const whitelist = [
        '/web/assets/',
        'web.assets_common',
        'web.assets_backend',
        'owl',
        '@web/',
        '@odoo-module',
        'odoo.define',
      ];
      const sLow = stack.toLowerCase();
      for (const w of whitelist) {
        if (sLow.includes(w.toLowerCase())) return false;
      }
      // Console indicators
      const consoleHints = ['<anonymous>:', ' at eval (', 'console._commandlineapi', 'injectedscript'];
      for (const h of consoleHints) {
        if (sLow.includes(h)) return true;
      }
      return false;
    } catch (_e) {
      return false;
    }
  }

  function shouldBlock(what) {
    if (!config.enabled || config.mode === 'off') return false;
    const fromConsole = isConsoleExecution();
    if (!fromConsole) return false;
    const msg = `🚫 [Console Security] BLOCKED: ${what} from console`;
    if (config.mode === 'logging') {
      console.warn(msg);
      return false;
    }
    console.error(msg);
    return true;
  }

  async function logClientError(ref, message, stack, meta) {
    try {
      await fetch('/web/console_security/log', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', method: 'call', params: { ref, message, stack, meta }, id: Math.floor(Math.random() * 1e6) }),
      });
    } catch (_e) {}
  }

  function sanitizeErrorDialogs() {
    if (config.isSupport) return; // allow full errors for support/admins

    const observer = new MutationObserver((mutations) => {
      for (const m of mutations) {
        for (const node of m.addedNodes) {
          if (!(node instanceof HTMLElement)) continue;
          // Heuristic: look for Odoo error dialogs
          const isDialog = node.classList && (node.classList.contains('o_dialog') || node.classList.contains('modal'));
          const text = node.textContent || '';
          const looksLikeOdooError = /Odoo (Client|Server) Error|See details|COPY THE FULL ERROR/i.test(text);
          if (isDialog && looksLikeOdooError) {
            try {
              const ref = (Date.now().toString(36) + Math.random().toString(36).slice(2, 8)).toUpperCase();
              // Replace dialog content with friendly message
              node.querySelectorAll('button, a').forEach((el) => {
                if (/see details|copy the full error/i.test(el.textContent || '')) {
                  el.style.display = 'none';
                }
              });
              // Replace header/body if present
              const titleEl = node.querySelector('.modal-title, .o_dialog_title');
              if (titleEl) titleEl.textContent = 'An error occurred';
              const bodyEl = node.querySelector('.modal-body, .o_dialog_content');
              if (bodyEl) {
                bodyEl.innerHTML = '';
                const p = document.createElement('p');
                p.textContent = 'Something went wrong. Please try again or contact support with Reference ID: ' + ref;
                bodyEl.appendChild(p);
              }
              // Log the original error text for support with ref id
              logClientError(ref, 'Client-side sanitized error dialog', text.slice(0, 2000), { url: location.href });
            } catch (_e) {}
          }
        }
      }
    });

    observer.observe(document.body, { childList: true, subtree: true });
  }

  function init() {
    if (window.__CONSOLE_SECURITY_MINIMAL__) return;
    window.__CONSOLE_SECURITY_MINIMAL__ = true;

    // 1) Block alert()
    try {
      const _alert = window.alert;
      window.alert = function () {
        if (shouldBlock('alert()')) return; // swallow
        return _alert.apply(this, arguments);
      };
    } catch (_e) {}

    // 2) Block document.cookie writes (setter only)
    try {
      const cookieDesc = Object.getOwnPropertyDescriptor(Document.prototype, 'cookie') ||
        Object.getOwnPropertyDescriptor(HTMLDocument.prototype, 'cookie');
      if (cookieDesc && cookieDesc.set) {
        Object.defineProperty(document, 'cookie', {
          get: cookieDesc.get,
          set: function (val) {
            if (typeof val === 'string' && shouldBlock('document.cookie set')) return false;
            return cookieDesc.set.call(this, val);
          },
          configurable: true,
        });
      }
    } catch (_e) {}

    // 3) Block javascript: navigations via location.href
    try {
      const hrefDesc = Object.getOwnPropertyDescriptor(Location.prototype, 'href') ||
        Object.getOwnPropertyDescriptor(location, 'href');
      if (hrefDesc && hrefDesc.set) {
        Object.defineProperty(location, 'href', {
          get: hrefDesc.get,
          set: function (value) {
            if (typeof value === 'string' && value.trim().toLowerCase().startsWith('javascript:')) {
              if (shouldBlock("location.href='javascript:'")) return; // swallow
            }
            return hrefDesc.set.call(this, value);
          },
          configurable: true,
        });
      }
    } catch (_e) {}

    // 4) Sanitize Odoo error dialogs for non-support users
    sanitizeErrorDialogs();

    console.log('[Console Security] Minimal protection + friendly errors active (', config.mode, ', support:', config.isSupport, ')');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      loadConfig().then(() => { init(); });
    });
  } else {
    loadConfig().then(() => { init(); });
  }
})();

