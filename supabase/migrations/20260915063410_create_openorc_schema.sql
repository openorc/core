-- Establish the dedicated OpenOrc application schema.
--
-- OpenOrc-owned control-plane tables live in the `openorc` schema, separate
-- from Supabase-managed schemas (auth, storage, ...) and from `public`.
-- Backend processes connect over direct Postgres with the platform owner
-- role; browser clients never read OpenOrc workflow data through Supabase
-- Data APIs, so no access is granted to browser-facing roles.
--
-- OpenOrc tables are added to this schema by later, individually reviewed
-- migrations. This migration only establishes and locks down the schema.

create schema if not exists openorc;

-- Least-privilege posture: nothing gains access by default. The schema owner
-- (the role backend processes connect as) retains full control through
-- ownership. Access for any other role can only be introduced by a
-- deliberate, reviewed migration that grants it explicitly.

revoke all on schema openorc from public;
revoke all on schema openorc from anon;
revoke all on schema openorc from authenticated;
