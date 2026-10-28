import { defineConfig } from 'vitest/config';
export default defineConfig({
  test: { globalSetup: ['test/global-setup.ts'], setupFiles: ['test/setup-env.ts'], testTimeout: 20_000 },
});
