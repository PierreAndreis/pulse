import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { CursorPresence } from "./CursorPresence.js";

// The docs page is static HTML; we just mount the live-presence overlay on top,
// so cursors/trails/selection/follow work here too — on the "/docs.html" channel.
createRoot(document.getElementById("presence-root")!).render(
  <StrictMode>
    <CursorPresence />
  </StrictMode>,
);
