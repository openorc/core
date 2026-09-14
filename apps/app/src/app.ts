// OpenOrc product SPA composition root.
//
// Wires the application plugin set: Pinia (registered for genuine client
// state; no stores exist yet), Vue Router, PrimeVue (Aura preset), and
// TanStack Vue Query (server-derived state).

import { createApp, type App as VueApp } from 'vue'
import { createPinia } from 'pinia'
import { VueQueryPlugin } from '@tanstack/vue-query'
import PrimeVue from 'primevue/config'
import Aura from '@primevue/themes/aura'
import type { Router } from 'vue-router'
import App from './App.vue'
import { router as defaultRouter } from './router'

export interface OpenOrcAppOptions {
  /** Router override, used by tests to swap the history implementation. */
  router?: Router
}

export interface OpenOrcApp {
  app: VueApp
  router: Router
}

export function createOpenOrcApp(options: OpenOrcAppOptions = {}): OpenOrcApp {
  const router = options.router ?? defaultRouter
  const app = createApp(App)

  app.use(createPinia())
  app.use(router)
  app.use(PrimeVue, {
    theme: {
      preset: Aura,
    },
  })
  app.use(VueQueryPlugin)

  return { app, router }
}