-- Establish the dedicated OpenOrc application schema.
--
-- OpenOrc-owned control-plane tables live in the `openorc` schema, separate
-- from Supabase-managed schemas (auth, storage, ...) and from `public`.
-- Browser clients never read OpenOrc workflow data through Supabase Data
-- APIs, so no access is granted to browser-facing roles.
--
-- OpenOrc tables are added to this schema by later, individually reviewed
-- migrations. This migration only establishes and locks down the schema.

create schema if not exists openorc;

-- Least-privilege posture: nothing gains access by default. The schema owner
-- retains full control through ownership. Any other runtime role requires a
-- deliberate, explicitly reviewed grant in a later migration.

revoke all on schema openorc from public;
revoke all on schema openorc from anon;
revoke all on schema openorc from authenticated;
