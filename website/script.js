document.documentElement.classList.add("js");

const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const revealElements = document.querySelectorAll(".reveal");
if ("IntersectionObserver" in window && !prefersReducedMotion) {
  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting) {
          entry.target.classList.add("visible");
          observer.unobserve(entry.target);
        }
      }
    },
    { threshold: 0.12, rootMargin: "0px 0px -40px 0px" }
  );
  for (const element of revealElements) {
    observer.observe(element);
  }
} else {
  for (const element of revealElements) {
    element.classList.add("visible");
  }
}

const header = document.querySelector(".site-header");
const onScrollHeader = () => {
  header.classList.toggle("scrolled", window.scrollY > 24);
};
onScrollHeader();
window.addEventListener("scroll", onScrollHeader, { passive: true });

const heroBg = document.querySelector(".hero-bg");
if (heroBg && !prefersReducedMotion) {
  let ticking = false;
  window.addEventListener(
    "scroll",
    () => {
      if (ticking) {
        return;
      }
      ticking = true;
      window.requestAnimationFrame(() => {
        const offset = Math.min(window.scrollY, window.innerHeight);
        heroBg.style.transform = `translateY(${offset * 0.12}px)`;
        ticking = false;
      });
    },
    { passive: true }
  );
}

const menuBtn = document.querySelector(".menu-btn");
const siteNav = document.querySelector(".site-nav");
if (menuBtn && siteNav) {
  menuBtn.addEventListener("click", () => {
    const open = siteNav.classList.toggle("open");
    menuBtn.setAttribute("aria-expanded", String(open));
    menuBtn.setAttribute("aria-label", open ? "Close menu" : "Open menu");
  });
  for (const link of siteNav.querySelectorAll("a")) {
    link.addEventListener("click", () => {
      siteNav.classList.remove("open");
      menuBtn.setAttribute("aria-expanded", "false");
      menuBtn.setAttribute("aria-label", "Open menu");
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && siteNav.classList.contains("open")) {
      siteNav.classList.remove("open");
      menuBtn.setAttribute("aria-expanded", "false");
      menuBtn.setAttribute("aria-label", "Open menu");
      menuBtn.focus();
    }
  });
}

// ---------- motion layer ----------

// Stagger card entrances within each grid.
for (const grid of document.querySelectorAll(".grid")) {
  let i = 0;
  for (const element of grid.querySelectorAll(":scope > .reveal")) {
    element.style.setProperty("--rd", `${Math.min(i, 8) * 70}ms`);
    i += 1;
  }
}

// Cursor spotlight on cards (fine pointers only; CSS hover is the fallback).
const finePointer = window.matchMedia("(pointer: fine)").matches;
if (finePointer && !prefersReducedMotion) {
  for (const card of document.querySelectorAll(".card")) {
    card.addEventListener("pointermove", (event) => {
      const rect = card.getBoundingClientRect();
      card.style.setProperty("--mx", `${event.clientX - rect.left}px`);
      card.style.setProperty("--my", `${event.clientY - rect.top}px`);
    });
  }
}

// Count-up hero stats on first view. Markup already holds final values,
// so no-motion and no-JS paths show the truth without doing anything.
const statValues = document.querySelectorAll(".hero-stats dd");
if (statValues.length > 0 && !prefersReducedMotion && "IntersectionObserver" in window) {
  const formatCount = (n) => n.toLocaleString("en-US");
  const counter = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) {
          continue;
        }
        const dd = entry.target;
        counter.unobserve(dd);
        const numNode = dd.firstChild;
        const target = parseInt(numNode.textContent.replace(/[^0-9]/g, ""), 10);
        if (Number.isNaN(target) || target <= 0) {
          continue;
        }
        const startedAt = performance.now();
        const duration = 1300;
        const tick = (now) => {
          const k = Math.min((now - startedAt) / duration, 1);
          const eased = 1 - Math.pow(1 - k, 3);
          numNode.textContent = `${formatCount(Math.round(target * eased))} `;
          if (k < 1) {
            requestAnimationFrame(tick);
          } else {
            numNode.textContent = `${formatCount(target)} `;
          }
        };
        requestAnimationFrame(tick);
      }
    },
    { threshold: 0.4 }
  );
  for (const dd of statValues) {
    counter.observe(dd);
  }
}

// Terminal types itself line-by-line on first view.
const terminal = document.querySelector(".hero-terminal");
const termCode = terminal ? terminal.querySelector(".terminal-body code") : null;
if (terminal && termCode && !prefersReducedMotion && "IntersectionObserver" in window) {
  const lines = termCode.innerHTML.split("\n");
  termCode.innerHTML = lines
    .map((line, i) => `<span class="tline" style="animation-delay:${(0.15 + i * 0.22).toFixed(2)}s">${line || " "}</span>`)
    .join("");
  terminal.classList.add("armed");
  const termObserver = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting) {
          terminal.classList.add("live");
          termObserver.disconnect();
        }
      }
    },
    { threshold: 0.35 }
  );
  termObserver.observe(terminal);
}

// Scrollspy: underline the nav link of the section in view.
const spyLinks = [...document.querySelectorAll(".site-nav a[href^='#']")];
if (spyLinks.length > 0 && "IntersectionObserver" in window) {
  const spyMap = new Map(spyLinks.map((a) => [a.getAttribute("href").slice(1), a]));
  const spy = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        const link = spyMap.get(entry.target.id);
        if (link && entry.isIntersecting) {
          for (const a of spyLinks) {
            a.classList.remove("active");
          }
          link.classList.add("active");
        }
      }
    },
    { rootMargin: "-40% 0px -55% 0px" }
  );
  for (const id of spyMap.keys()) {
    const section = document.getElementById(id);
    if (section) {
      spy.observe(section);
    }
  }
}
