<script setup lang="ts">
// Bootstrap placeholder view. Product screens (dashboard, Task wizards,
// Owner Requests, telemetry) are deliberately out of scope for this
// milestone.
</script>

<template>
  <section class="home-view">
    <h1>OpenOrc</h1>
    <p class="home-view-tagline">
      Open, provider-neutral control plane for governed agentic software
      development.
    </p>
    <p class="home-view-status">
      SPA foundation bootstrapped; product surfaces arrive in later milestones.
    </p>
  </section>
</template>

<style scoped>
.home-view-tagline {
  max-width: 40rem;
}

.home-view-status {
  color: var(--p-text-muted-color, #64748b);
}
</style>