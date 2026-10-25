# GitHub Adapter Agent Context

## Boundary

The GitHub adapter owns provider API/webhook mechanics, typed normalization, stable external identity, and authoritative observations. Application services own Task authority, gates, lifecycle, retries, and workflow consequences.

## Authentication and transport

- Installation authorization is for App installation/repository discovery, capability checks, and authoritative reads/reconciliation. Owner-accountable engineering-record writes require the exact Profile-scoped GitHub App user-to-server token; never fall back to installation authentication, PATs, sign-in tokens, or another user's authorization.
- The adapter never resolves or refreshes Profile-scoped user credentials. Token lifecycle/identity proof is a service boundary. GitHub App bootstrap keys and secrets remain deployment-only; transient tokens are secret-bearing and redacted.
- Pin the documented GitHub REST API version centrally. Use documented provider operations and exact stable identity checks; mutable URLs/names are navigation/address data, not authority.
- Follow redirects only for safe read methods and only within the trusted HTTPS API origin, with bounded hops. Never replay mutating operations on a redirect or uncertain outcome.
- Distinguish definitive rejection, rate limits (safe normalized retry metadata), and uncertain provider/transport outcomes. Never expose response bodies, secrets, headers, or untrusted provider text as application errors/log attributes.
- GitHub user-flow token exchange uses the documented HTTPS endpoint, no redirects, and correctly forwards a correlated PKCE verifier when present. OAuth code alone is not identity proof; authoritative current-user identity must be checked against the Profile.

## Reconciliation and PR safety

- Treat webhooks as hints to re-read authoritative GitHub state. Reconciliation tolerates replay, missed deliveries, and out-of-order notifications. Stable GitHub IDs and exact commit SHAs win over names/URLs.
- Branch and PR observations must match their addressed stable subjects. Read CI checks/status at exact head SHA, paginate sufficiently to prove completeness, and never implement a competing merge-policy engine.
- PR creation has no atomic expected-head parameter. Services enforce exact-head preflight and immediate post-create reconciliation; the adapter reports exactly one known success, definitive rejection (including proven duplicate), or uncertain outcome, without silently retrying or adopting an existing PR.
- Merge requests carry the expected reviewed head SHA. Distinguish confirmed merge, head mismatch, definitive GitHub rejection, and uncertainty. Only authoritative confirmation permits completion.

## Webhook ingress

- Verify the exact raw bytes with GitHub's SHA-256 HMAC using constant-time comparison before trusting or parsing payload fields. Missing/invalid signatures reject uniformly at transport; never store bodies or secrets.
- Classify only the supported event families and stable installation/repository/issue/PR routing identifiers. Installation-scoped deliveries fan out through explicit routed repositories; cross-Workspace routing is separately reauthorized. Provider event names and content never become domain-level authority.
- Deduplicate by delivery GUID before effects; persist only bounded safe delivery classification and routing. A valid but irrelevant delivery may be acknowledged without workflow effect.
- Never treat issue comments, labels, webhook contents, or agent prose as commands granting Producer authority.

Read services/persistence guidance for mutations and `src/openorc/api/AGENTS.md` for webhook HTTP routes.
