const articleBody = document.querySelector(".article-body");
const tocTarget = document.querySelector("#article-toc-links");

if (articleBody && tocTarget) {
  const headings = [...articleBody.querySelectorAll("h2, h3")];
  const list = document.createElement("ol");

  headings.forEach((heading) => {
    if (!heading.id) return;
    const item = document.createElement("li");
    const link = document.createElement("a");
    link.href = `#${heading.id}`;
    link.textContent = heading.textContent;
    if (heading.tagName === "H3") item.className = "article-toc__subitem";
    item.append(link);
    list.append(item);
  });

  if (list.children.length) tocTarget.append(list);
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
