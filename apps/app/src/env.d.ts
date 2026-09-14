// Custom Vite environment variables exposed to the client bundle.
// Vite only injects VITE_-prefixed variables.

interface ImportMetaEnv {
  /**
   * Absolute http(s) base URL of the OpenOrc API.
   * Must be configured explicitly; there is no same-origin fallback.
   */
  readonly VITE_API_BASE_URL?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}