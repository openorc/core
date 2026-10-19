-- OwnerGate REVIEW_RESOLUTION settlement (issue #211): the terminal
-- ``settled`` status is the Reviewer-driven settlement outcome for
-- review_resolution gates only. Owner-driven APPROVED/REJECTED/CANCELLED
-- semantics and every other column, constraint, index, and privilege are
-- untouched.

-- Replace the status vocabulary CHECK (auto-named ``owner_gates_status_check``
-- at table creation): ``settled`` joins the terminal vocabulary, and only a
-- review_resolution gate may ever carry it.
alter table openorc.owner_gates
    drop constraint owner_gates_status_check;

alter table openorc.owner_gates
    add constraint owner_gates_status_check
    check (
        status in ('pending', 'approved', 'rejected', 'cancelled')
        or (status = 'settled' and gate_type = 'review_resolution')
    );

-- Replace the decided-at coherence CHECK (auto-named ``owner_gates_check1``
-- at table creation) under an explicit name: the terminal stamp is non-NULL
-- exactly when the status is not pending, settled included.
alter table openorc.owner_gates
    drop constraint owner_gates_check1;

alter table openorc.owner_gates
    add constraint owner_gates_decided_at_coherence_check
    check (
        (status = 'pending' and decided_at is null)
        or (
            status in ('approved', 'rejected', 'cancelled', 'settled')
            and decided_at is not null
        )
    );
