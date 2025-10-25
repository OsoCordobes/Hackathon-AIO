const chat = document.getElementById("chat");
const msg = document.getElementById("msg");
const sendBtn = document.getElementById("send");
const suggest = document.getElementById("suggest");

let history = [];

function addMessage(role, text) {
  const row = document.createElement("div");
  row.className = `msg ${role}`;
  const av = document.createElement("div");
  av.className = `avatar ${role}`;
  av.textContent = role === "user" ? "U" : "J";
  const b = document.createElement("div");
  b.className = "bubble";
  b.textContent = text;
  if (role === "user") {
    row.appendChild(b);
    row.appendChild(av);
  } else {
    row.appendChild(av);
    row.appendChild(b);
  }
  chat.appendChild(row);
  chat.scrollTop = chat.scrollHeight;
}

function setSuggestions(items) {
  suggest.innerHTML = "";
  (items || []).forEach((s) => {
    const b = document.createElement("button");
    b.textContent = s;
    b.onclick = () => {
      msg.value = s;
      send();
    };
    suggest.appendChild(b);
  });
}

async function send() {
  const text = msg.value.trim();
  if (!text) return;
  addMessage("user", text);
  msg.value = "";
  setSuggestions([]);

  addMessage("bot", "…"); // placeholder
  const placeholder = chat.lastChild.querySelector(".bubble");

  try {
    const r = await fetch("/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text, history }),
    });
    const j = await r.json();
    placeholder.textContent = j.text || "No reply.";
    setSuggestions(j.suggestions);
    history.push({ role: "user", content: text });
    history.push({ role: "assistant", content: j.text || "" });
  } catch (e) {
    placeholder.textContent = "Error contacting the planner.";
  }
}

sendBtn.onclick = send;
msg.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    send();
  }
});

// greet
addMessage(
  "bot",
  "I'm online. Example: 'product_556490 is missing' or 'plant_253 is not working'."
);
setSuggestions(["product_556490 is missing", "plant_253 is not working"]);
