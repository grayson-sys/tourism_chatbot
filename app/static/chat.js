const form = document.getElementById("chat-form");
const input = document.getElementById("chat-input");
const chatWindow = document.getElementById("chat-window");
const submitButton = document.getElementById("chat-submit");
const statusEl = document.getElementById("chat-status");

function escapeHtml(value) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeAndLinkify(text) {
  const escaped = escapeHtml(text);
  return escaped.replace(/(https?:\/\/[^\s]+)/g, '<a href="$1" target="_blank" rel="noopener">$1</a>');
}

function formatItinerary(text) {
  const lines = text.split(/\n/);
  const html = [];
  const dayRows = [];
  let currentDay = null;
  let currentBlocks = [];

  const flushDay = () => {
    if (!currentDay) return;
    const content = currentBlocks
      .filter(Boolean)
      .map((item) => `<p>${escapeAndLinkify(item)}</p>`)
      .join("");
    dayRows.push(
      `<tr><th>${escapeHtml(currentDay)}</th><td>${content}</td></tr>`
    );
    currentDay = null;
    currentBlocks = [];
  };

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (/^Day\\s+\\d+/i.test(trimmed)) {
      flushDay();
      currentDay = trimmed.replace(/:$/, "");
      continue;
    }
    if (/^(Trip summary|Day-by-day itinerary|New Mexico True Certified picks|Practical notes)/i.test(trimmed)) {
      flushDay();
      html.push(`<h3>${escapeHtml(trimmed.replace(/:$/, ""))}</h3>`);
      continue;
    }
    if (currentDay) {
      currentBlocks.push(trimmed.replace(/^[-*]\\s+/, ""));
    } else {
      html.push(`<p>${escapeAndLinkify(trimmed)}</p>`);
    }
  }
  flushDay();

  if (dayRows.length) {
    html.push(`<table class="itinerary-table">${dayRows.join("")}</table>`);
  }
  return html.join("");
}

function addMessage(text, className) {
  const div = document.createElement("div");
  div.className = className;
  chatWindow.classList.remove("is-empty");
  div.textContent = text;
  chatWindow.appendChild(div);
  chatWindow.scrollTop = chatWindow.scrollHeight;
  return div;
}

async function streamChat(payload, assistantEl) {
  const basePath = document.body?.dataset?.basePath || "";
  const response = await fetch(`${basePath}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const message = await response.text();
    assistantEl.textContent = `Error: ${message}`;
    if (statusEl) {
      statusEl.textContent = `Error: ${message || "Request failed"}`;
      statusEl.classList.add("error");
    }
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let fullText = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    fullText += decoder.decode(value, { stream: true });
    assistantEl.textContent = fullText;
    chatWindow.scrollTop = chatWindow.scrollHeight;
  }

  assistantEl.innerHTML = formatItinerary(fullText);
  assistantEl.classList.add("sources");
  if (statusEl) {
    statusEl.textContent = "Done.";
    statusEl.classList.remove("error");
  }

  const sourceUrls = Array.from(fullText.matchAll(/https?:\/\/[^\s)\]]+/g)).map(
    (match) => match[0]
  );
  if (sourceUrls.length) {
    try {
      const resp = await fetch(`${basePath}/api/source-images`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ urls: sourceUrls }),
      });
      if (resp.ok) {
        const data = await resp.json();
        if (data.images && data.images.length) {
          const gallery = document.createElement("div");
          gallery.className = "photo-strip";
          for (const image of data.images.slice(0, 3)) {
            const figure = document.createElement("figure");
            const img = document.createElement("img");
            img.src = image.image_url;
            img.alt = image.title || "Source image";
            const caption = document.createElement("figcaption");
            caption.textContent = image.title || image.url;
            figure.appendChild(img);
            figure.appendChild(caption);
            gallery.appendChild(figure);
          }
          assistantEl.appendChild(gallery);
        }
      }
    } catch (err) {
      console.warn("Photo strip failed", err);
    }
  }
}

async function handleSubmit() {
  const message = input.value.trim();
  if (!message) return;

  submitButton.disabled = true;
  if (statusEl) {
    statusEl.classList.add("active");
    statusEl.classList.remove("error");
    statusEl.textContent = "Thinking… 0s";
  }
  const start = Date.now();
  const timer = setInterval(() => {
    if (statusEl) {
      const seconds = Math.floor((Date.now() - start) / 1000);
      statusEl.textContent = `Thinking… ${seconds}s`;
    }
  }, 1000);

  addMessage(message, "user-message");
  const assistantEl = addMessage("", "assistant-message");

  const payload = {
    message,
  };

  input.value = "";

  try {
    await streamChat(payload, assistantEl);
  } catch (err) {
    assistantEl.textContent = "Something went wrong. Please try again.";
    if (statusEl) {
      statusEl.textContent = "Error: Request failed.";
      statusEl.classList.add("error");
    }
  } finally {
    clearInterval(timer);
    submitButton.disabled = false;
    if (statusEl) {
      statusEl.classList.remove("active");
    }
  }
}

if (submitButton) {
  submitButton.addEventListener("click", handleSubmit);
}

if (input) {
  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      handleSubmit();
    }
  });
}
