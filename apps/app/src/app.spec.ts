// Bootstrap composition test: exercises the real application composition
// root (Pinia + Router + PrimeVue + Vue Query) against the real route table,
// not a bare App.vue mount.

import { afterEach, describe, expect, it } from 'vitest'
import { nextTick } from 'vue'
import { createMemoryHistory } from 'vue-router'
import { createOpenOrcApp } from './app'
import { createAppRouter } from './router'

describe('OpenOrc app composition', () => {
  let container: HTMLElement | undefined
  let mountedApp: { unmount: () => void } | undefined

  afterEach(() => {
    mountedApp?.unmount()
    container?.remove()
    container = undefined
    mountedApp = undefined
  })

  it('installs Pinia, Router, PrimeVue, and Vue Query and renders the root route', async () => {
    container = document.createElement('div')
    container.id = 'app'
    document.body.appendChild(container)

    const { app, router } = createOpenOrcApp({
      router: createAppRouter(createMemoryHistory()),
    })
    mountedApp = app

    await router.isReady()
    app.mount(container)
    await nextTick()

    expect(router.currentRoute.value.name).toBe('home')
    expect(container.querySelector('.app-shell-header')).not.toBeNull()
    expect(container.querySelector('.home-view')).not.toBeNull()
    expect(container.textContent).toContain('OpenOrc')
    expect(container.textContent).toContain('governed agentic software development')
  })
})