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
