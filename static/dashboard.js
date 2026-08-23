/* StellarGate dashboard.
 *
 * A thin client over the same public REST API documented in the README: it
 * holds no privileged session of its own and adds no server-side state. The
 * merchant's API key lives in the browser and is sent as a bearer token.
 *
 * Every value that originates from the API is written with textContent (or
 * via el()//setText below), never innerHTML. `webhook_url`, `memo` and the
 * event name are merchant-controlled, so interpolating them as markup would
 * be a stored-XSS vector.
 */

(function () {
  "use strict";

  var API_BASE = "/v1";
  var PAGE_SIZE = 25;
  var KEY_NAME = "stellargate.apiKey";

  var state = {
    key: null,
    status: "",
    cursor: null,
    loading: false,
  };

  // ── Tiny DOM helpers ──────────────────────────────────────────────────

  function $(id) {
    return document.getElementById(id);
  }

  /** Create an element with a class and *text* content (never markup). */
  function el(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined && text !== null) node.textContent = String(text);
    return node;
  }

  function show(node, visible) {
    node.hidden = !visible;
  }

  function clear(node) {
    while (node.firstChild) node.removeChild(node.firstChild);
  }

  function setError(node, message) {
    if (message) {
      node.textContent = message;
      show(node, true);
    } else {
      node.textContent = "";
      show(node, false);
    }
  }

  // ── Formatting ────────────────────────────────────────────────────────

  function fmtTime(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    return isNaN(d.getTime()) ? iso : d.toLocaleString();
  }

  function shortId(id) {
    return typeof id === "string" && id.length > 12 ? id.slice(0, 8) + "…" : id;
  }

  /** Map a payment or delivery status onto a pill style. */
  function pillClass(status) {
    switch (status) {
      case "completed":
      case "delivered":
        return "pill pill-ok";
      case "pending":
      case "underpaid":
        return "pill pill-warn";
      case "expired":
      case "failed":
        return "pill pill-err";
      default:
        return "pill pill-idle";
    }
  }

  // ── API ───────────────────────────────────────────────────────────────

  /**
   * Call the gateway. Resolves with the parsed body, or rejects with an Error
   * carrying the API's `error` message when one is present. A 401 drops the
   * stored key and returns to the sign-in gate, since it means the key was
   * revoked or is wrong.
   */
  function api(path, options) {
    var opts = options || {};
    var headers = { Accept: "application/json" };
    if (state.key) headers.Authorization = "Bearer " + state.key;

    return fetch(API_BASE + path, { method: opts.method || "GET", headers: headers }).then(
      function (res) {
        if (res.status === 401) {
          signOut("That API key was rejected. Please sign in again.");
          throw new Error("unauthorized");
        }
        return res
          .json()
          .catch(function () {
            return {};
          })
          .then(function (body) {
            if (!res.ok) {
              throw new Error(body.error || "Request failed (" + res.status + ")");
            }
            return body;
          });
      }
    );
  }

  // ── Session ───────────────────────────────────────────────────────────

  function storedKey() {
    try {
      return (
        window.sessionStorage.getItem(KEY_NAME) ||
        window.localStorage.getItem(KEY_NAME)
      );
    } catch (e) {
      return null; // storage blocked; fall back to in-memory only
    }
  }

  function storeKey(key, persist) {
    try {
      (persist ? window.localStorage : window.sessionStorage).setItem(
        KEY_NAME,
        key
      );
    } catch (e) {
      /* non-fatal: the key still works for this page load */
    }
  }

  function forgetKey() {
    try {
      window.sessionStorage.removeItem(KEY_NAME);
      window.localStorage.removeItem(KEY_NAME);
    } catch (e) {
      /* nothing to do */
    }
  }

  /** Return to the sign-in form, keeping any stored key so a reload retries. */
  function showGate(message) {
    state.key = null;
    closeDetail();
    show($("app"), false);
    show($("gate"), true);
    setError($("gate-error"), message || null);
  }

  /** Return to the sign-in form AND discard the stored key.
   *
   * Only for cases where the key itself is the problem (a 401, or an explicit
   * sign-out). A transient failure must use showGate() instead: discarding a
   * perfectly good key because the network blinked forces the user to dig it
   * out again. */
  function signOut(message) {
    forgetKey();
    showGate(message);
  }

  function signIn(key, persist) {
    state.key = key;
    // Validate by making the cheapest authenticated call available.
    return api("/payments?limit=1").then(function () {
      if (persist !== null) storeKey(key, persist);
      show($("gate"), false);
      show($("app"), true);
      setError($("gate-error"), null);
      loadVersion();
      pollHealth();
      reload();
    });
  }

  // ── Payments list ─────────────────────────────────────────────────────

  function reload() {
    state.cursor = null;
    clear($("rows"));
    loadPayments();
  }

  function loadPayments() {
    if (state.loading) return;
    state.loading = true;
    setError($("list-error"), null);

    var query = "/payments?limit=" + PAGE_SIZE;
    if (state.status) query += "&status=" + encodeURIComponent(state.status);
    if (state.cursor) query += "&cursor=" + encodeURIComponent(state.cursor);

    api(query)
      .then(function (body) {
        var payments = body.payments || [];
        payments.forEach(appendRow);

        // The offset-mode response returns a cursor even on the final page, so
        // a short page is what actually signals the end.
        var more = payments.length === PAGE_SIZE && !!body.next_cursor;
        state.cursor = more ? body.next_cursor : null;
        show($("load-more"), more);
        show($("empty"), $("rows").childElementCount === 0);
      })
      .catch(function (err) {
        if (err.message !== "unauthorized") setError($("list-error"), err.message);
      })
      .then(function () {
        state.loading = false;
      });
  }

  function appendRow(p) {
    var tr = document.createElement("tr");
    tr.tabIndex = 0;

    var statusCell = document.createElement("td");
    statusCell.appendChild(el("span", pillClass(p.status), p.status));
    tr.appendChild(statusCell);

    tr.appendChild(el("td", null, p.amount + " " + p.asset));
    tr.appendChild(el("td", "mono", p.memo));
    tr.appendChild(el("td", null, fmtTime(p.created_at)));
    tr.appendChild(el("td", "mono", shortId(p.id)));

    tr.addEventListener("click", function () {
      openDetail(p.id);
    });
    tr.addEventListener("keydown", function (ev) {
      if (ev.key === "Enter" || ev.key === " ") {
        ev.preventDefault();
        openDetail(p.id);
      }
    });

    $("rows").appendChild(tr);
  }

  // ── Detail panel ──────────────────────────────────────────────────────

  function openDetail(id) {
    show($("detail"), true);
    show($("scrim"), true);

    var fields = $("detail-fields");
    clear(fields);
    clear($("deliveries"));
    setError($("deliveries-error"), null);
    show($("deliveries-empty"), false);

    api("/payments/" + encodeURIComponent(id))
      .then(function (p) {
        [
          ["Status", p.status],
          ["Amount", p.amount + " " + p.asset],
          ["Received", p.paid_amount ? p.paid_amount + " " + p.asset : "—"],
          ["Memo", p.memo],
          ["Destination", p.destination_address],
          ["Transaction", p.tx_hash || "—"],
          ["Payment ID", p.id],
          ["Merchant", p.merchant_id],
          ["Created", fmtTime(p.created_at)],
          ["Updated", fmtTime(p.updated_at)],
          ["Expires", fmtTime(p.expires_at)],
        ].forEach(function (pair) {
          fields.appendChild(el("dt", null, pair[0]));
          if (pair[0] === "Status") {
            var dd = document.createElement("dd");
            dd.appendChild(el("span", pillClass(p.status), p.status));
            fields.appendChild(dd);
          } else {
            fields.appendChild(el("dd", "mono", pair[1]));
          }
        });
      })
      .catch(function (err) {
        if (err.message !== "unauthorized") {
          fields.appendChild(el("dd", "error", err.message));
        }
      });

    loadDeliveries(id);
  }

  function loadDeliveries(paymentId) {
    api("/payments/" + encodeURIComponent(paymentId) + "/webhooks")
      .then(function (body) {
        var list = $("deliveries");
        clear(list);
        var deliveries = body.deliveries || [];
        show($("deliveries-empty"), deliveries.length === 0);
        deliveries.forEach(function (d) {
          list.appendChild(deliveryItem(paymentId, d));
        });
      })
      .catch(function (err) {
        if (err.message !== "unauthorized") {
          setError($("deliveries-error"), err.message);
        }
      });
  }

  function deliveryItem(paymentId, d) {
    var li = el("li", "delivery");

    var head = el("div", "delivery-head");
    head.appendChild(el("strong", null, d.event || "webhook"));
    head.appendChild(el("span", pillClass(d.status), d.status));
    li.appendChild(head);

    li.appendChild(el("div", "mono", d.url));
    li.appendChild(
      el(
        "div",
        "delivery-meta",
        "attempt " + d.attempts + " · last " + fmtTime(d.last_attempt)
      )
    );

    var button = el("button", "ghost", "Redeliver");
    button.addEventListener("click", function () {
      button.disabled = true;
      button.textContent = "Sending…";
      api(
        "/payments/" +
          encodeURIComponent(paymentId) +
          "/webhooks/" +
          encodeURIComponent(d.id) +
          "/redeliver",
        { method: "POST" }
      )
        .then(function () {
          loadDeliveries(paymentId);
        })
        .catch(function (err) {
          button.disabled = false;
          button.textContent = "Redeliver";
          if (err.message !== "unauthorized") {
            setError($("deliveries-error"), err.message);
          }
        });
    });
    li.appendChild(button);

    return li;
  }

  function closeDetail() {
    show($("detail"), false);
    show($("scrim"), false);
  }

  // ── Version ───────────────────────────────────────────────────────────

  /** The root route answers with "StellarGate API vX.Y.Z". */
  function loadVersion() {
    fetch("/")
      .then(function (res) {
        return res.text();
      })
      .then(function (text) {
        var match = /v\d+\.\d+\.\d+/.exec(text);
        if (match) $("version").textContent = match[0];
      })
      .catch(function () {
        /* cosmetic only */
      });
  }

  // ── Health ────────────────────────────────────────────────────────────

  function pollHealth() {
    fetch("/ready", { headers: { Accept: "application/json" } })
      .then(function (res) {
        return res.json().then(function (body) {
          return { ok: res.ok, body: body };
        });
      })
      .then(function (r) {
        var pill = $("health");
        pill.className = r.ok ? "pill pill-ok" : "pill pill-err";
        pill.textContent = r.ok ? "healthy" : r.body.reason || "unavailable";
      })
      .catch(function () {
        var pill = $("health");
        pill.className = "pill pill-err";
        pill.textContent = "unreachable";
      });
  }

  // ── Wiring ────────────────────────────────────────────────────────────

  function init() {
    $("gate-form").addEventListener("submit", function (ev) {
      ev.preventDefault();
      var key = $("api-key").value.trim();
      if (!key) return;
      setError($("gate-error"), null);
      signIn(key, $("remember").checked).catch(function (err) {
        if (err.message !== "unauthorized") setError($("gate-error"), err.message);
      });
    });

    $("sign-out").addEventListener("click", function () {
      signOut(null);
    });

    $("refresh").addEventListener("click", reload);
    $("load-more").addEventListener("click", loadPayments);
    $("detail-close").addEventListener("click", closeDetail);
    $("scrim").addEventListener("click", closeDetail);

    document.addEventListener("keydown", function (ev) {
      if (ev.key === "Escape") closeDetail();
    });

    Array.prototype.forEach.call(
      document.querySelectorAll(".chip"),
      function (chip) {
        chip.addEventListener("click", function () {
          Array.prototype.forEach.call(
            document.querySelectorAll(".chip"),
            function (c) {
              c.className = "chip";
            }
          );
          chip.className = "chip chip-on";
          state.status = chip.getAttribute("data-status") || "";
          reload();
        });
      }
    );

    window.setInterval(function () {
      if (state.key) pollHealth();
    }, 30000);

    // Resume an existing session when a key is already stored.
    /* Resume an existing session when a key is already stored. The gate is
       visible until this succeeds, so any failure here simply leaves the user
       looking at the sign-in form rather than at nothing. */
    var existing = storedKey();
    if (existing) {
      signIn(existing, null).catch(function (err) {
        /* A 401 already returned to the gate via signOut() inside api(). Every
           other failure — server down, network dropped, a proxy returning a
           login page — must land there too. Swallowing it would leave both
           panels hidden and render a blank page with no way forward. */
        if (err.message !== "unauthorized") {
          showGate("Could not restore your session: " + err.message);
        }
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
