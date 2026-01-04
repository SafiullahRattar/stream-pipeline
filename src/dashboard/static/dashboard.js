/**
 * Real-time dashboard WebSocket client.
 *
 * Connects to the pipeline's WebSocket endpoint, receives aggregation
 * snapshots, and updates the DOM. Reconnects automatically on disconnect.
 */

(function () {
    "use strict";

    const WS_RECONNECT_DELAY_MS = 2000;
    const MAX_LOG_ENTRIES = 50;

    // DOM references
    const statusDot = document.getElementById("statusDot");
    const statusText = document.getElementById("statusText");
    const totalEventsEl = document.getElementById("totalEvents");
    const eventsPerSecEl = document.getElementById("eventsPerSec");
    const totalRevenueEl = document.getElementById("totalRevenue");
    const purchasesEl = document.getElementById("purchases");
    const uniqueUsersEl = document.getElementById("uniqueUsers");
    const clientsEl = document.getElementById("clients");
    const eventTypeBarsEl = document.getElementById("eventTypeBars");
    const categoryBarsEl = document.getElementById("categoryBars");
    const eventLogEl = document.getElementById("eventLog");

    let ws = null;
    let lastTotalEvents = 0;
    let lastUpdateTime = Date.now();

    function formatNumber(n) {
        if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
        if (n >= 1_000) return (n / 1_000).toFixed(1) + "K";
        return n.toLocaleString();
    }

    function formatCurrency(dollars) {
        return "$" + dollars.toLocaleString("en-US", {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        });
    }

    function formatTime(ms) {
        const d = new Date(ms);
        return d.toLocaleTimeString();
    }

    function setConnected(connected) {
        if (connected) {
            statusDot.classList.add("connected");
            statusText.textContent = "Connected";
        } else {
            statusDot.classList.remove("connected");
            statusText.textContent = "Disconnected";
        }
    }

    function updateKPIs(data) {
        const totals = data.totals || {};
        const now = Date.now();
        const elapsed = (now - lastUpdateTime) / 1000;

        const currentTotal = totals.events || 0;
        const eps = elapsed > 0 ? (currentTotal - lastTotalEvents) / elapsed : 0;
        lastTotalEvents = currentTotal;
        lastUpdateTime = now;

        totalEventsEl.textContent = formatNumber(currentTotal);
        eventsPerSecEl.textContent = Math.round(eps) + " events/sec";
        totalRevenueEl.textContent = formatCurrency(totals.revenue_dollars || 0);
        purchasesEl.textContent = formatNumber(totals.purchases || 0) + " purchases";
        uniqueUsersEl.textContent = formatNumber(totals.unique_users || 0);
        clientsEl.textContent = data.connected_clients || 0;
    }

    function updateEventTypeBars(latest) {
        if (!latest || !latest.event_count) return;

        const types = {
            page_view: latest.page_views || 0,
            search: (latest.event_count || 0) - (latest.page_views || 0) -
                    (latest.add_to_carts || 0) - (latest.purchases || 0),
            add_to_cart: latest.add_to_carts || 0,
            purchase: latest.purchases || 0,
            wishlist: 0,
        };

        // Clamp search to non-negative
        if (types.search < 0) types.search = 0;

        const maxVal = Math.max(...Object.values(types), 1);

        let html = "";
        for (const [name, count] of Object.entries(types)) {
            const pct = (count / maxVal) * 100;
            html += `
                <div class="bar-row">
                    <div class="bar-label">${name}</div>
                    <div class="bar-track">
                        <div class="bar-fill" style="width:${pct}%"></div>
                    </div>
                    <div class="bar-value">${formatNumber(count)}</div>
                </div>`;
        }
        eventTypeBarsEl.innerHTML = html;
    }

    function updateCategories(latest) {
        if (!latest || !latest.top_categories) return;
        const cats = latest.top_categories.slice(0, 5);
        if (cats.length === 0) {
            categoryBarsEl.innerHTML = '<div style="color:var(--text-secondary)">Waiting for data...</div>';
            return;
        }

        let html = "";
        for (let i = 0; i < cats.length; i++) {
            const pct = ((cats.length - i) / cats.length) * 100;
            html += `
                <div class="bar-row">
                    <div class="bar-label">${cats[i]}</div>
                    <div class="bar-track">
                        <div class="bar-fill" style="width:${pct}%; background:var(--accent-purple)"></div>
                    </div>
                    <div class="bar-value">#${i + 1}</div>
                </div>`;
        }
        categoryBarsEl.innerHTML = html;
    }

    function addLogEntry(agg) {
        const entry = document.createElement("div");
        entry.className = "entry";

        const windowType = agg.window_type || "unknown";
        const revenueDollars = (agg.total_revenue_cents || 0) / 100;

        entry.innerHTML = `
            <span class="ts">${formatTime(agg.emitted_at)}</span>
            <span class="type">${windowType}</span>
            <span class="detail">
                ${formatNumber(agg.event_count)} events |
                ${formatNumber(agg.unique_users)} users |
                ${formatCurrency(revenueDollars)} revenue
            </span>`;

        eventLogEl.prepend(entry);

        // Trim old entries
        while (eventLogEl.children.length > MAX_LOG_ENTRIES) {
            eventLogEl.removeChild(eventLogEl.lastChild);
        }
    }

    function handleMessage(event) {
        try {
            const data = JSON.parse(event.data);
            updateKPIs(data);
            updateEventTypeBars(data.latest);
            updateCategories(data.latest);

            // Add latest to log if it has data
            if (data.latest && data.latest.event_count > 0) {
                addLogEntry(data.latest);
            }
        } catch (err) {
            console.error("Failed to parse WebSocket message:", err);
        }
    }

    function connect() {
        const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
        const url = `${proto}//${window.location.host}/ws`;

        ws = new WebSocket(url);

        ws.onopen = function () {
            console.log("WebSocket connected");
            setConnected(true);
        };

        ws.onmessage = handleMessage;

        ws.onclose = function () {
            console.log("WebSocket disconnected, reconnecting...");
            setConnected(false);
            setTimeout(connect, WS_RECONNECT_DELAY_MS);
        };

        ws.onerror = function (err) {
            console.error("WebSocket error:", err);
            ws.close();
        };
    }

    // Keepalive ping
    setInterval(function () {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send("ping");
        }
    }, 30000);

    // Start
    connect();
})();
