<script setup lang="ts">
// Minimal application shell. Product surfaces (dashboard, Task wizards,
// Owner Requests, telemetry) are implemented in later milestones; this shell
// only proves composition, routing, and rendering.
</script>

<template>
  <div class="app-shell">
    <header class="app-shell-header">
      <span class="app-shell-brand">OpenOrc</span>
    </header>
    <main class="app-shell-main">
      <router-view />
    </main>
  </div>
</template>

<style scoped>
.app-shell {
  display: flex;
  flex-direction: column;
  min-height: 100dvh;
}

.app-shell-header {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--p-content-border-color, #e2e8f0);
}

.app-shell-brand {
  font-weight: 600;
}

.app-shell-main {
  flex: 1;
  width: 100%;
  max-width: 72rem;
  margin: 0 auto;
  padding: 1rem;
}
</style>