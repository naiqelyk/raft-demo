import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The UI lives in src/ui. It also imports the scenario scripts from test/util,
// which Vite allows because they're inside the project root.
export default defineConfig({
  root: "src/ui",
  plugins: [react()],
  build: { outDir: "../../dist", emptyOutDir: true },
});
