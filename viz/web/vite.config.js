import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const backend = process.env.VIZ_BACKEND ?? 'localhost:8088';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/ws': { target: `ws://${backend}`, ws: true },
      '/health': { target: `http://${backend}` },
    },
  },
  test: {
    environment: 'node',
  },
});
