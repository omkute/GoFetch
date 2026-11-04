// GoFetch v2 — Modern Desktop/Web GUI Controller

(function () {
  "use strict";

  // Application State
  const state = {
    tasks: [],
    statusFilter: "all",
    categoryFilter: "all",
    speedHistory: new Array(60).fill(0),
    eventSource: null,
  };

  // DOM Elements
  const headerSpeed = document.getElementById("header-speed");
  const headerActive = document.getElementById("header-active");
  const rateLimitSelect = document.getElementById("global-rate-limit-select");
  const tasksTable = document.getElementById("tasks-table");
  const tasksTbody = document.getElementById("tasks-tbody");
  const emptyState = document.getElementById("empty-state");
  const listCount = document.getElementById("list-count");
  const listTitle = document.getElementById("list-title");
  const addModal = document.getElementById("add-modal");
  const addForm = document.getElementById("add-form");
  const speedChartCanvas = document.getElementById("speed-chart");

  // Chart Rendering Context
  const ctx = speedChartCanvas.getContext("2d");

  // Helpers
  function formatBytes(bytes) {
    if (bytes <= 0 || isNaN(bytes)) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + " " + units[i];
  }

  function formatETA(seconds) {
    if (seconds <= 0 || isNaN(seconds)) return "--";
    if (seconds < 60) return `${seconds}s`;
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    if (mins < 60) return `${mins}m ${secs}s`;
    const hrs = Math.floor(mins / 60);
    return `${hrs}h ${mins % 60}m`;
  }

  function showToast(msg) {
    const container = document.getElementById("toast-container");
    const toast = document.createElement("div");
    toast.className = "toast";
    toast.textContent = msg;
    container.appendChild(toast);
    setTimeout(() => {
      toast.remove();
    }, 3000);
  }

  // Real-Time Canvas Speed History Chart
  function renderChart() {
    const w = speedChartCanvas.parentElement.clientWidth;
    const h = 120;
    speedChartCanvas.width = w * window.devicePixelRatio;
    speedChartCanvas.height = h * window.devicePixelRatio;
    ctx.scale(window.devicePixelRatio, window.devicePixelRatio);

    ctx.clearRect(0, 0, w, h);

    const data = state.speedHistory;
    const maxVal = Math.max(...data, 1024 * 1024); // at least 1 MB/s max axis

    // Draw Grid Lines
    ctx.strokeStyle = "rgba(48, 54, 61, 0.4)";
    ctx.lineWidth = 1;
    for (let i = 1; i <= 3; i++) {
      const y = (h / 4) * i;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(w, y);
      ctx.stroke();
    }

    // Draw Line Chart with Gradient Fill
    ctx.beginPath();
    const step = w / (data.length - 1);
    data.forEach((val, i) => {
      const x = i * step;
      const y = h - (val / maxVal) * (h - 10) - 5;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });

    // Stroke
    ctx.strokeStyle = "#58a6ff";
    ctx.lineWidth = 2;
    ctx.stroke();

    // Fill under curve
    ctx.lineTo(w, h);
    ctx.lineTo(0, h);
    ctx.closePath();
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, "rgba(88, 166, 255, 0.25)");
    grad.addColorStop(1, "rgba(88, 166, 255, 0.0)");
    ctx.fillStyle = grad;
    ctx.fill();
  }

  // Update UI State & Badges
  function updateUI() {
    let totalSpeed = 0;
    let activeCount = 0;
    const counts = {
      all: state.tasks.length,
      downloading: 0,
      queued: 0,
      paused: 0,
      completed: 0,
      failed: 0,
    };

    const catCounts = {
      Videos: 0,
      Audio: 0,
      Documents: 0,
      Archives: 0,
      Software: 0,
      Images: 0,
      Miscellaneous: 0,
    };

    state.tasks.forEach((t) => {
      if (counts[t.status] !== undefined) counts[t.status]++;
      if (t.status === "downloading") {
        activeCount++;
        totalSpeed += t.speed_bps || 0;
      }
      if (t.category && catCounts[t.category] !== undefined) {
        catCounts[t.category]++;
      } else {
        catCounts["Miscellaneous"]++;
      }
    });

    // Push into speed history
    state.speedHistory.shift();
    state.speedHistory.push(totalSpeed);

    headerSpeed.textContent = formatBytes(totalSpeed) + "/s";
    headerActive.textContent = activeCount;

    // Update filter badges
    document.getElementById("badge-all").textContent = counts.all;
    document.getElementById("badge-downloading").textContent = counts.downloading;
    document.getElementById("badge-queued").textContent = counts.queued;
    document.getElementById("badge-paused").textContent = counts.paused;
    document.getElementById("badge-completed").textContent = counts.completed;
    document.getElementById("badge-failed").textContent = counts.failed;

    // Update category badges
    document.getElementById("cat-videos").textContent = catCounts.Videos;
    document.getElementById("cat-audio").textContent = catCounts.Audio;
    document.getElementById("cat-docs").textContent = catCounts.Documents;
    document.getElementById("cat-archives").textContent = catCounts.Archives;
    document.getElementById("cat-software").textContent = catCounts.Software;
    document.getElementById("cat-images").textContent = catCounts.Images;
    document.getElementById("cat-misc").textContent = catCounts.Miscellaneous;

    renderTasksTable();
    renderChart();
  }

  // Render Table
  function renderTasksTable() {
    const filtered = state.tasks.filter((t) => {
      if (state.statusFilter !== "all" && t.status !== state.statusFilter) return false;
      if (state.categoryFilter !== "all") {
        if (state.categoryFilter === "Miscellaneous" && !t.category) return true;
        if (t.category !== state.categoryFilter) return false;
      }
      return true;
    });

    listCount.textContent = `${filtered.length} items`;
    if (filtered.length === 0) {
      emptyState.style.display = "block";
      tasksTable.style.display = "none";
      return;
    }

    emptyState.style.display = "none";
    tasksTable.style.display = "table";

    tasksTbody.innerHTML = "";
    filtered.forEach((t) => {
      const tr = document.createElement("tr");

      // Status Badge Class
      let statusClass = "badge";
      if (t.status === "downloading") statusClass = "badge badge-accent";
      else if (t.status === "completed") statusClass = "badge badge-success";
      else if (t.status === "paused") statusClass = "badge badge-warning";
      else if (t.status === "failed") statusClass = "badge badge-danger";

      // Priority Class
      const prioClass = t.priority === "high" ? "badge badge-accent" : "badge";

      // Progress bar
      const pct = (t.progress_percent || 0).toFixed(1);

      tr.innerHTML = `
        <td>
          <div class="task-file" title="${t.file_name}">${t.file_name || "Detecting..."}</div>
          <div class="task-url" title="${t.url}">${t.url}</div>
        </td>
        <td><span class="${statusClass}">${t.status}</span></td>
        <td><span class="${prioClass}">${(t.priority || "normal").toUpperCase()}</span></td>
        <td>
          <div class="progress-bar-container">
            <div class="progress-bar-fill" style="width: ${pct}%"></div>
          </div>
          <span class="progress-text">${pct}% (${formatBytes(t.downloaded_bytes)} / ${formatBytes(t.total_bytes)})</span>
        </td>
        <td>
          <div class="speed-metric">${t.status === "downloading" ? formatBytes(t.speed_bps) + "/s" : "--"}</div>
          <div class="eta-metric">${t.status === "downloading" ? "ETA: " + formatETA(t.eta_seconds) : ""}</div>
        </td>
        <td>
          ${renderActions(t)}
        </td>
      `;
      tasksTbody.appendChild(tr);
    });
  }

  function renderActions(task) {
    if (task.status === "downloading" || task.status === "queued") {
      return `
        <button class="btn btn-secondary btn-sm" onclick="window.GoFetchActions.pause('${task.id}')">Pause</button>
        <button class="btn btn-secondary btn-sm" onclick="window.GoFetchActions.delete('${task.id}')">✕</button>
      `;
    }
    if (task.status === "paused" || task.status === "failed") {
      return `
        <button class="btn btn-secondary btn-sm" onclick="window.GoFetchActions.resume('${task.id}')">Resume</button>
        <button class="btn btn-secondary btn-sm" onclick="window.GoFetchActions.delete('${task.id}')">✕</button>
      `;
    }
    return `
      <button class="btn btn-secondary btn-sm" onclick="window.GoFetchActions.delete('${task.id}')">Remove</button>
    `;
  }

  // Global Action Handlers
  window.GoFetchActions = {
    pause: async function (id) {
      try {
        await fetch(`/api/v1/downloads/${id}/pause`, { method: "POST" });
        showToast("Download paused");
      } catch (e) {
        showToast("Error pausing: " + e.message);
      }
    },
    resume: async function (id) {
      try {
        await fetch(`/api/v1/downloads/${id}/resume`, { method: "POST" });
        showToast("Download resumed");
      } catch (e) {
        showToast("Error resuming: " + e.message);
      }
    },
    delete: async function (id) {
      try {
        await fetch(`/api/v1/downloads/${id}?delete_files=true`, { method: "DELETE" });
        showToast("Download deleted");
      } catch (e) {
        showToast("Error deleting: " + e.message);
      }
    },
  };

  // SSE Stream Connection
  function connectSSE() {
    if (state.eventSource) state.eventSource.close();

    const es = new EventSource("/api/v1/events");
    state.eventSource = es;

    es.addEventListener("progress", (e) => {
      try {
        const data = JSON.parse(e.data);
        if (data.downloads) {
          state.tasks = data.downloads;
          updateUI();
        }
      } catch (err) {
        console.error("SSE parse error", err);
      }
    });

    es.onerror = () => {
      // Reconnection fallback
      setTimeout(pollFallback, 2000);
    };
  }

  // Fallback Polling if SSE drops
  async function pollFallback() {
    try {
      const resp = await fetch("/api/v1/downloads");
      if (resp.ok) {
        const data = await resp.json();
        state.tasks = data.items || [];
        updateUI();
      }
    } catch (_) {}
  }

  // Initialize Event Listeners
  function initListeners() {
    // Filter clicks
    document.querySelectorAll("#status-filters .nav-item").forEach((el) => {
      el.addEventListener("click", () => {
        document.querySelectorAll("#status-filters .nav-item").forEach((i) => i.classList.remove("active"));
        el.classList.add("active");
        state.statusFilter = el.dataset.status;
        renderTasksTable();
      });
    });

    document.querySelectorAll("#category-filters .nav-item").forEach((el) => {
      el.addEventListener("click", () => {
        document.querySelectorAll("#category-filters .nav-item").forEach((i) => i.classList.remove("active"));
        el.classList.add("active");
        state.categoryFilter = el.dataset.category;
        renderTasksTable();
      });
    });

    // Pause all / Resume all
    document.getElementById("btn-pause-all").addEventListener("click", async () => {
      await fetch("/api/v1/downloads/pause-all", { method: "POST" });
      showToast("Paused all downloads");
    });

    document.getElementById("btn-resume-all").addEventListener("click", async () => {
      await fetch("/api/v1/downloads/resume-all", { method: "POST" });
      showToast("Resumed all downloads");
    });

    // Global Rate Limit
    rateLimitSelect.addEventListener("change", async (e) => {
      const limit = e.target.value;
      await fetch("/api/v1/system/rate-limit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rate_limit: limit }),
      });
      showToast(`Speed limit set to: ${limit === "0" ? "Uncapped" : limit}`);
    });

    // Modal Add
    document.getElementById("btn-open-modal").addEventListener("click", () => {
      addModal.style.display = "flex";
      document.getElementById("input-url").focus();
    });

    const closeModal = () => {
      addModal.style.display = "none";
      addForm.reset();
    };
    document.getElementById("modal-close-btn").addEventListener("click", closeModal);
    document.getElementById("modal-cancel-btn").addEventListener("click", closeModal);

    addForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const rawText = document.getElementById("input-url").value.trim();
      const lines = rawText.split("\n").map((s) => s.trim()).filter((s) => s.length > 0);
      const priority = document.getElementById("input-priority").value;
      const chunks = parseInt(document.getElementById("input-chunks").value, 10) || 4;
      const rateLimit = document.getElementById("input-rate").value.trim();
      const destDir = document.getElementById("input-dest").value.trim();

      if (lines.length === 0) return;

      if (lines.length === 1) {
        // Single download
        await fetch("/api/v1/downloads", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            url: lines[0],
            priority,
            max_chunks: chunks,
            rate_limit: rateLimit,
            destination_dir: destDir,
          }),
        });
        showToast("Enqueued download");
      } else {
        // Batch download
        await fetch("/api/v1/downloads/batch", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            urls: lines,
            priority,
            max_chunks: chunks,
            rate_limit: rateLimit,
            destination_dir: destDir,
          }),
        });
        showToast(`Enqueued batch of ${lines.length} downloads`);
      }

      closeModal();
      pollFallback();
    });

    window.addEventListener("resize", renderChart);
  }

  // Boot
  document.addEventListener("DOMContentLoaded", () => {
    initListeners();
    connectSSE();
    pollFallback();
    setInterval(renderChart, 1000);
  });
})();
