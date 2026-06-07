import { useEffect, useState } from "react";

// Minimal client router: pushState + a synthetic popstate so hooks re-read the
// path. Enough for a two-page site without pulling in a routing library.
export function navigate(to: string) {
  const current = location.pathname + location.search + location.hash;
  if (to === current) return;
  history.pushState(null, "", to);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

/** Re-renders on navigation; returns the current pathname. */
export function usePath(): string {
  const [path, setPath] = useState(location.pathname);
  useEffect(() => {
    const on = () => setPath(location.pathname);
    window.addEventListener("popstate", on);
    return () => window.removeEventListener("popstate", on);
  }, []);
  return path;
}

/** The two real routes; everything else falls back to the landing page. */
export function isDocs(path: string): boolean {
  return path === "/docs.html" || path === "/docs";
}
