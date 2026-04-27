(function () {
    "use strict";

    document.addEventListener("DOMContentLoaded", function () {

        var params       = new URLSearchParams(window.location.search);
        var isSupportUrl = params.get("support") === "1";

        // ── detect AD state ─────────────────────────────────────────────
        var adSection   = document.querySelector(".o_login_auth");
        var adContainer = adSection && adSection.querySelector(".o_auth_oauth_providers");
        var adActive    = adContainer && adContainer.querySelectorAll("a").length > 0;

        // ── grab elements ────────────────────────────────────────────────
        var toggleWrap   = document.getElementById("ad-toggle-wrap");
        var btnToggle    = document.getElementById("btn-toggle-login");
        var normalEmail  = document.getElementById("normal-email-wrap");
        var normalPass   = document.getElementById("normal-password-wrap");
        var normalSubmit = document.getElementById("normal-submit-btn");
        var normalFields = [normalEmail, normalPass, normalSubmit];

        // ── no AD configured → leave form visible, nothing to do ─────────
        if (!adActive) {
            return;
        }

        // ── AD is active → ALWAYS hide the normal form for everyone ──────
        normalFields.forEach(function (el) {
            if (el) el.style.display = "none";
        });

        // ── support URL only → reveal the toggle button ──────────────────
        if (isSupportUrl) {
            if (toggleWrap) toggleWrap.style.display = "";

            if (btnToggle) {
                btnToggle.innerHTML = `
                    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16"
                        viewBox="0 0 24 24" fill="none" stroke="currentColor"
                        stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
                        style="flex-shrink:0;opacity:0.6;">
                        <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
                        <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                    </svg>
                    <span>Sign in with standard login</span>
                `;
            }

            // ── toggle handler (support only) ─────────────────────────────
            var isAD = true;

            function setMode(showAD) {
                isAD = showAD;

                normalFields.forEach(function (el) {
                    if (el) el.style.display = showAD ? "none" : "";
                });

                if (adSection) adSection.style.display = showAD ? "" : "none";

                var label = btnToggle && btnToggle.querySelector("span");
                if (label) {
                    label.textContent = showAD
                        ? "Sign in with standard login"
                        : "Sign in with Active Directory";
                }
            }

            if (btnToggle) {
                btnToggle.addEventListener("click", function () {
                    setMode(!isAD);
                });
            }
        }

    });

}());