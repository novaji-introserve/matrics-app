odoo.define("session_control.SessionCheck", function (require) {
  "use strict";

  var ajax = require("web.ajax");
  var session = require("web.session");
  var Dialog = require("web.Dialog");

  // Tab identification logic remains the same
  const myCurrentInstanceId = (Math.random() + 1).toString(36).substring(2);
  localStorage.setItem("session_control_active_tab_id", myCurrentInstanceId);
  sessionStorage.setItem("session_control_my_tab_id", myCurrentInstanceId);

  var logoutInProgress = false;
  var checkInterval = 5000; // Check every 5 seconds

  /**
   * A function to display a styled Odoo dialog and then FORCIBLY log the user out.
   * @param {string} title - The title for the dialog box.
   * @param {string} message - The main text content for the dialog.
   */
  function showLogoutDialog(title, message) {
    if (logoutInProgress) return;
    logoutInProgress = true;

    // Create the dialog instance but don't open it yet.
    const dialog = new Dialog(null, {
      title: title,
      size: "medium",
      $content: $("<p>", { text: message }),
      buttons: [
        {
          text: "OK",
          classes: "btn-primary",
          close: true, // This will trigger the 'closed' event we are listening for.
        },
      ],
    });

    // This function will be executed WHENEVER the dialog is closed,
    // whether by the 'OK' button, the 'X' button, or the Escape key.
    dialog.on("closed", null, function () {
      // Force the redirection to the logout page.
      window.location.href = "/web/session/logout?redirect=/web/login";
    });

    // Now, open the dialog.
    dialog.open();
  }

  setInterval(function () {
    if (session.uid && !logoutInProgress) {
      const myTabId = sessionStorage.getItem("session_control_my_tab_id");
      const activeTabId = localStorage.getItem("session_control_active_tab_id");

      // Check 1: Same-Browser Tab Conflict
      if (activeTabId !== myTabId) {
        showLogoutDialog(
          "Session Terminated",
          "You have opened iComply in a new tab or window. This session will be closed."
        );
        return;
      }

      // Check 2: Different-Browser/Device Conflict
      ajax
        .jsonRpc("/web/session/validate_custom", "call", {})
        .then(function (result) {
          if (result && result.valid === false) {
            showLogoutDialog(
              "Session Terminated",
              "Your session was terminated because your account was logged in from another browser or location."
            );
          }
        });
    }
  }, checkInterval);
});
