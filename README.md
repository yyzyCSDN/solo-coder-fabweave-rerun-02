# FabWeave Operations Lab

FabWeave is a dependency-light Python reference project for semiconductor-fab
operations.  Its production modules model queue-time protection, dispatch,
reticle reservations, recipe qualification, SPC, metrology sampling,
maintenance, chamber matching, holds, genealogy, chemical aging, cleanroom
excursions, demand control, rework, lot merging, equipment dedication,
contamination, takt, route yield, shipment commitments and cross-plant
transfer scan chains.

The cross-domain workflow layer composes these services for material
readiness, dispatch-after-hold, qualified reservations and assignments,
metrology/SPC gates, containment, powered scheduling, rework release,
merge-to-commit, performance reporting and trusted transfer scanning.
The transfer chain (`loaded` -> `departed` -> `arrived` -> `unloaded`)
advances only in order, replays repeated scans without double booking,
requires a recorded reason to skip a stage, refuses factory exit while a
material hold is active, and links the received material back into the
original genealogy only after a complete, quantity-conserving transfer.
It preserves domain rejection reasons instead of replacing them with a
generic success or failure.

Shared infrastructure supplies half-open time intervals, versioned policy
history, immutable decisions, quantity conservation and an append-only event
log with optimistic concurrency.  Domain services use these primitives to keep
event-time, idempotency and fail-closed behavior consistent.

Run the tests with `python -m pytest -q`.
