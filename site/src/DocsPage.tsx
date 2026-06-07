import { useEffect } from "react";
import docsHtml from "./docs-content.html?raw";

// The docs are static content; we inject the existing markup verbatim so nothing
// has to be ported to JSX. Styling comes from /styles.css (.docs-wrap etc.).
export function DocsPage() {
  useEffect(() => {
    document.title = "Docs — Pulse";
    if (location.hash) {
      const el = document.querySelector(location.hash);
      if (el) setTimeout(() => el.scrollIntoView(), 0);
    }
  }, []);
  return <div dangerouslySetInnerHTML={{ __html: docsHtml }} />;
}
