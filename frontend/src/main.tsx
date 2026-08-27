import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import App from "./App";
import "./styles.css";

const rootElement = document.getElementById("root");

if (!rootElement) {
  throw new Error("RetryPermit could not find the application root.");
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
