package queue

import (
	"testing"

	"github.com/atoonk/packetio"
)

// TestStalledDriverNeedsAForcedTransmit reproduces the deadlock a driver with
// no working completion notification falls into, and checks that
// AllocForce/TransmitForce is what gets it out.
//
// virtio-net implements neither rte_eth_tx_done_cleanup nor a
// tx_descriptor_status with a reclaiming side effect (unlike mlx5, which the
// package comment on PMD.Poke already documents as relying on the latter): its
// only place completions are ever read is inside a live, non-empty TxBurst.
// Once a queue's own bookkeeping (NumFreeSlots) reaches Depth in flight,
// ordinary Alloc refuses every further call before it can ever reach
// Transmit -- so nothing is left to call TxBurst again, and the queue is
// stuck forever even though the driver may have room. Measured on a real
// deployment: a generator asking for hundreds of thousands of packets/second
// sent exactly Depth (1024) and then achieved a few hundred packets/second
// for the rest of every trial, indefinitely.
func TestStalledDriverNeedsAForcedTransmit(t *testing.T) {
	const depth = 4
	r := newRig(t, 64)
	q := r.tx(t, poolA, 0, 64, depth)
	r.pmd.pokeBroken = true
	// A driver like virtio reads its completions as part of any transmit that
	// carries at least one real packet -- no batch threshold to clear first,
	// unlike the mlx5-shaped fake the other tests use.
	r.pmd.compThresh = 1

	send := func(n int) int {
		descs := append([]packetio.Desc(nil), q.Alloc(n)...)
		for i := range descs {
			descs[i].Len = 64
		}
		sent := q.Transmit(descs)
		q.Free(descs[sent:])
		return sent
	}

	// Fill the ring exactly to its configured depth, as an unlucky burst of
	// legitimate traffic would.
	if sent := send(depth); sent != depth {
		t.Fatalf("filling the ring sent %d, want %d", sent, depth)
	}
	if got := q.NumFreeSlots(); got != 0 {
		t.Fatalf("NumFreeSlots = %d after filling the ring, want 0", got)
	}

	// Ordinary reclaim cannot help: Poke is broken, and nothing has driven a
	// real transmit since the ring filled, so nothing is in Returned either.
	if got := q.Complete(1024); got != 0 {
		t.Fatalf("Complete reclaimed %d frames from a broken Poke, want 0", got)
	}
	if got := q.NumFreeSlots(); got != 0 {
		t.Fatalf("NumFreeSlots = %d after a no-op Complete, want still 0", got)
	}

	// This is the deadlock: plain Alloc refuses forever from here, because
	// nothing before it ever reaches Transmit to give the driver another
	// chance to reclaim.
	if descs := q.Alloc(1); descs != nil {
		t.Fatalf("Alloc succeeded against a full ring; the fake driver needs adjusting to still reproduce the deadlock")
	}

	// AllocForce + TransmitForce draws against the frame pool instead of the
	// ring's own bookkeeping, so it goes through -- and because it is a real,
	// non-empty TxBurst, it is what finally gives the driver a chance to read
	// its completions.
	descs := q.AllocForce(1)
	if len(descs) != 1 {
		t.Fatalf("AllocForce(1) returned %d descriptors, want 1 (pool exhausted?)", len(descs))
	}
	descs[0].Len = 64
	sent := q.TransmitForce(descs)
	if sent != 1 {
		t.Fatalf("TransmitForce sent %d, want 1 (the fake driver should always accept)", sent)
	}

	// The forced send's own TxBurst call ran the driver's completion check
	// with the previously-stuck frames still held, so they are now in
	// Returned, and ordinary reclaim -- and ordinary Alloc -- work again.
	if got := q.Complete(1024); got != depth {
		t.Fatalf("Complete reclaimed %d frames after the forced send, want %d (the deadlock is not actually broken)", got, depth)
	}
	if got := q.NumInFlight(); got != 1 {
		t.Fatalf("NumInFlight = %d after reclaiming, want 1 (only the forced send itself outstanding)", got)
	}
	if descs := q.Alloc(1); descs == nil {
		t.Fatal("Alloc still refuses after the deadlock should be broken")
	}
}

// TestForcedAllocRespectsThePool checks the harmless half of the contract:
// AllocForce bypasses the ring-depth count, never the frame pool itself, so an
// exhausted pool still refuses cleanly instead of overrunning it.
func TestForcedAllocRespectsThePool(t *testing.T) {
	r := newRig(t, 8)
	q := r.tx(t, poolA, 0, 8, 4)

	first := q.AllocForce(8)
	if len(first) != 8 {
		t.Fatalf("AllocForce(8) returned %d, want 8 to exhaust the pool", len(first))
	}
	for i := range first {
		first[i].Len = 64
	}

	if descs := q.AllocForce(1); descs != nil {
		t.Fatalf("AllocForce succeeded against an exhausted pool: %d descriptors", len(descs))
	}

	// Return them, and confirm a subsequent AllocForce/TransmitForce pair
	// behaves normally end to end.
	q.Free(first)
	descs := q.AllocForce(1)
	if len(descs) != 1 {
		t.Fatalf("AllocForce(1) after freeing returned %d, want 1", len(descs))
	}
	descs[0].Len = 64
	if sent := q.TransmitForce(descs); sent != 1 {
		t.Fatalf("TransmitForce sent %d, want 1", sent)
	}
}
