// Externalized API base configuration for the OpenOrc product SPA.
//
// Invariant: the SPA never assumes it is served by the OpenOrc API process
// and never falls back silently to same-origin behavior. The API base URL is
// configured through VITE_API_BASE_URL; a missing or invalid value fails
// closed when the application needs it.

const ENV_KEY = 'VITE_API_BASE_URL'

export class ApiConfigError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ApiConfigError'
  }
}

export function getApiBaseUrl(): string {
  const raw = import.meta.env.VITE_API_BASE_URL
  const value = typeof raw === 'string' ? raw.trim() : ''

  if (value === '') {
    throw new ApiConfigError(
      `${ENV_KEY} is not configured. Set it to the OpenOrc API base URL (for example http://127.0.0.1:3000) in the environment; the SPA never assumes a same-origin API.`,
    )
  }

  let parsed: URL
  try {
    parsed = new URL(value)
  } catch {
    throw new ApiConfigError(`${ENV_KEY} is invalid: "${value}" is not an absolute http(s) URL.`)
  }

  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new ApiConfigError(`${ENV_KEY} is invalid: "${value}" must use the http or https scheme.`)
  }

  return parsed.href.replace(/\/+$/, '')
}