const articleBody = document.querySelector(".article-body");
const tocLinks = [...document.querySelectorAll(".article-toc a[href^='#']")];
const languageToggle = document.querySelector(".language-toggle");

if (languageToggle) {
  const currentLanguage = document.documentElement.lang || "en";
  const preferredLanguage = localStorage.getItem("streamhouse-language");

  languageToggle.addEventListener("click", () => {
    localStorage.setItem(
      "streamhouse-language",
      currentLanguage === "ko" ? "en" : "ko"
    );
  });

  if (preferredLanguage && preferredLanguage !== currentLanguage) {
    window.location.replace(languageToggle.href);
  }
}

// The table of contents is rendered at build time (_includes/toc.html).
// This only marks the section being read, for the sticky desktop list.
if (articleBody && tocLinks.length) {
  const sections = tocLinks
    .map((link) => [link, document.getElementById(decodeURIComponent(link.hash.slice(1)))])
    .filter(([, heading]) => heading);
  let current = null;
  let scheduled = false;

  const markCurrent = () => {
    scheduled = false;
    const line = window.innerHeight * 0.25;
    let next = null;
    for (const [link, heading] of sections) {
      if (heading.getBoundingClientRect().top > line) break;
      next = link;
    }
    const atEnd = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 2;
    if (atEnd && sections.length) next = sections[sections.length - 1][0];
    if (next === current) return;
    current?.removeAttribute("aria-current");
    next?.setAttribute("aria-current", "location");
    current = next;
  };

  const schedule = () => {
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(markCurrent);
  };

  window.addEventListener("scroll", schedule, { passive: true });
  window.addEventListener("resize", schedule);
  schedule();
}

if (articleBody) {
  const tables = [...articleBody.querySelectorAll("table")];

  tables.forEach((table, index) => {
    const block = document.createElement("div");
    const region = document.createElement("div");
    const hint = document.createElement("span");
    const columnCount = table.querySelectorAll("thead th").length;
    let heading = table.previousElementSibling;

    while (heading && !heading.matches("h2, h3")) {
      heading = heading.previousElementSibling;
    }

    block.className = "table-block";
    if (columnCount >= 5) block.classList.add("table-block--wide");

    region.className = "table-region";
    region.setAttribute("role", "region");
    region.setAttribute(
      "aria-label",
      `${heading?.textContent || `Table ${index + 1}`} data table`
    );

    hint.className = "table-block__hint";
    hint.setAttribute("aria-hidden", "true");
    hint.textContent = "Scroll horizontally to see all columns →";

    table.before(block);
    block.append(region, hint);
    region.append(table);

    const updateOverflow = () => {
      const overflowing = region.scrollWidth > region.clientWidth + 2;
      const atStart = region.scrollLeft <= 2;
      const atEnd = region.scrollLeft + region.clientWidth >= region.scrollWidth - 2;

      block.classList.toggle("is-overflowing", overflowing);
      block.classList.toggle("can-scroll-left", overflowing && !atStart);
      block.classList.toggle("can-scroll-right", overflowing && !atEnd);

      if (overflowing) region.tabIndex = 0;
      else region.removeAttribute("tabindex");
    };

    region.addEventListener("scroll", updateOverflow, { passive: true });
    if ("ResizeObserver" in window) {
      new ResizeObserver(updateOverflow).observe(region);
    }
    requestAnimationFrame(updateOverflow);
  });
}

const mermaidBlocks = document.querySelectorAll(
  "div.language-mermaid, pre > code.language-mermaid"
);

if (mermaidBlocks.length) {
  for (const block of mermaidBlocks) {
    const container = block.closest("div.language-mermaid") || block.parentElement;
    const diagram = document.createElement("pre");
    diagram.className = "mermaid";
    diagram.textContent = block.textContent;
    container.replaceWith(diagram);
  }

  const { default: mermaid } = await import(
    "https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.esm.min.mjs"
  );
  mermaid.initialize({
    startOnLoad: false,
    theme: "base",
    themeVariables: {
      background: "#ffffff",
      primaryColor: "#ecf5f3",
      primaryTextColor: "#132b35",
      primaryBorderColor: "#77a9a1",
      lineColor: "#66808a",
      secondaryColor: "#f4f7f8",
      tertiaryColor: "#fff7e8",
      fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    }
  });
  await mermaid.run({ querySelector: "pre.mermaid" });
}
