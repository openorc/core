// Deterministic contract tests for the externalized API base configuration.
// Invariant: a missing or invalid VITE_API_BASE_URL must fail closed instead
// of silently falling back to same-origin behavior.

import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiConfigError, getApiBaseUrl } from './apiConfig'

const envRecord = import.meta.env as Record<string, string | undefined>
const ORIGINAL_VALUE = envRecord.VITE_API_BASE_URL

function restoreEnv(): void {
  vi.unstubAllEnvs()
  if (ORIGINAL_VALUE === undefined) {
    delete envRecord.VITE_API_BASE_URL
  } else {
    envRecord.VITE_API_BASE_URL = ORIGINAL_VALUE
  }
}

describe('getApiBaseUrl', () => {
  afterEach(restoreEnv)

  it('returns the configured absolute http(s) base URL with a trailing slash normalized', () => {
    vi.stubEnv('VITE_API_BASE_URL', 'http://127.0.0.1:3000/')
    expect(getApiBaseUrl()).toBe('http://127.0.0.1:3000')
  })

  it('preserves a configured base path and normalizes its trailing slash', () => {
    vi.stubEnv('VITE_API_BASE_URL', 'https://api.example.com/openorc/')
    expect(getApiBaseUrl()).toBe('https://api.example.com/openorc')
  })

  it('fails closed when the variable is unset — no silent same-origin fallback', () => {
    delete envRecord.VITE_API_BASE_URL
    expect(() => getApiBaseUrl()).toThrow(ApiConfigError)
  })

  it('fails closed when the variable is configured as an empty string', () => {
    vi.stubEnv('VITE_API_BASE_URL', '')
    expect(() => getApiBaseUrl()).toThrow(/VITE_API_BASE_URL is not configured/)
  })

  it('fails closed when the variable is configured as whitespace only', () => {
    vi.stubEnv('VITE_API_BASE_URL', '   ')
    expect(() => getApiBaseUrl()).toThrow(/VITE_API_BASE_URL is not configured/)
  })

  it('fails closed when the variable is not an absolute http(s) URL', () => {
    vi.stubEnv('VITE_API_BASE_URL', 'api/relative-path')
    expect(() => getApiBaseUrl()).toThrow(/not an absolute http\(s\) URL/)
  })

  it('fails closed when the variable uses a non-http(s) scheme', () => {
    vi.stubEnv('VITE_API_BASE_URL', 'ftp://example.com')
    expect(() => getApiBaseUrl()).toThrow(/must use the http or https scheme/)
  })
})