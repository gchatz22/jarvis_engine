//! Graph quiescence detection for GC.
//!
//! An agent that has finished its cycle parks at the wake gate awaiting a
//! signal; it never self-completes. That is correct for continuous monitors and
//! re-propagation trees, but a `never`-cadence, single-invocation graph whose
//! triggers are all consumed reaches a state from which **no further signal can
//! ever originate** — yet its workflows stay alive forever. This module decides
//! when a graph has provably reached that fixpoint so the reaper can retire it.
//!
//! ## Governing invariant
//!
//! The danger is asymmetric. A false negative (lingering a quiescent graph)
//! costs a little Temporal state and self-corrects on the next sweep. A false
//! positive (retiring a graph with a wake still coming) drops that wake through
//! the best-effort, swallowed cross-workflow signal path — a silent correctness
//! bug. Every rule here biases to false negatives.
//!
//! ## Why the predicate is sound (and its scope)
//!
//! An idle agent sends no signals until something wakes it, so once *every*
//! agent is idle the graph's send-set is frozen — no new signal can originate.
//! The only residual is a signal sent just before its sender idled, not yet
//! received; [`GraphQuiescence`] catches it by requiring the observation to hold
//! stable across a window longer than a full propagation wave (a bump in any
//! agent's cumulative-received counter resets the clock). This holds *only* for
//! `never` + exhausted-trigger graphs: with a cadence timer or a re-propagation
//! source, "all idle" is not a fixpoint, so the scope gate ([`agent_quiescent`]
//! requires `is_never`) is load-bearing, not incidental.

use std::time::Duration;

use crate::workflow::AgentSnapshot;

/// One agent's identity plus its latest snapshot, as fed to
/// [`GraphQuiescence::observe`].
#[derive(Clone, Debug)]
pub struct AgentObservation {
    pub agent_id: String,
    pub snapshot: AgentSnapshot,
}

/// Whether one agent is *instantaneously* quiescent: a `never`-cadence agent
/// parked at the wake gate, past its forced first cycle, with every pending
/// signal bucket empty.
///
/// `is_never` is the scope gate (see module docs). `parked` excludes an agent
/// mid-cycle — including one burning an inspection loop with empty queues, which
/// is busy, not idle. `tick >= 1` means the agent is past its forced first wake,
/// so no self-wake timer is armed. Instantaneous only: stability across a
/// propagation wave is [`GraphQuiescence`]'s job.
pub fn agent_quiescent(s: &AgentSnapshot) -> bool {
    s.is_never
        && s.parked
        && s.tick >= 1
        && s.pending_triggers_count == 0
        && s.pending_human_ops_count == 0
        && s.pending_mandate_patches_count == 0
}

/// Per-agent cumulative-received counts, keyed by agent id, sorted for a
/// membership- and order-stable comparison. A change in any count — or in the
/// agent set itself — signals activity within the window and resets the clock.
type Digest = Vec<(String, u64, u64, u64)>;

fn digest(agents: &[AgentObservation]) -> Digest {
    let mut d: Digest = agents
        .iter()
        .map(|a| {
            (
                a.agent_id.clone(),
                a.snapshot.cumulative_triggers_observed,
                a.snapshot.cumulative_human_ops_observed,
                a.snapshot.cumulative_mandate_patches_observed,
            )
        })
        .collect();
    d.sort();
    d
}

/// The reaper's decision for one graph after an observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The graph has held quiescent for at least `wave_margin`; retire it.
    Retire,
    /// Not (yet) retire-eligible — keep observing.
    Wait,
}

/// Debounced quiescence detector for a single graph.
///
/// Fed one observation per sweep via [`Self::observe`]. It returns
/// [`Verdict::Retire`] only once every agent has been continuously quiescent —
/// same agent set, unchanged cumulative counters — across a span of at least
/// `wave_margin`. Any non-quiescent sample or any counter/membership change
/// resets the clock. Purely in-memory: a fresh tracker (e.g. after a worker
/// restart) simply re-earns the full window, which is false-negative-biased.
#[derive(Clone, Debug)]
pub struct GraphQuiescence {
    wave_margin: Duration,
    stable_since: Option<Duration>,
    last_digest: Option<Digest>,
}

impl GraphQuiescence {
    /// `wave_margin` must exceed the time for a full root-ward propagation wave
    /// in the graph, so an in-flight signal that will re-wake an ancestor has
    /// time to bump a counter and reset the clock before the window elapses.
    pub fn new(wave_margin: Duration) -> Self {
        Self {
            wave_margin,
            stable_since: None,
            last_digest: None,
        }
    }

    /// Record the graph's state at monotonic time `now` (e.g. elapsed since the
    /// reaper started). `now` must be non-decreasing across calls.
    pub fn observe(&mut self, now: Duration, agents: &[AgentObservation]) -> Verdict {
        let all_quiescent =
            !agents.is_empty() && agents.iter().all(|a| agent_quiescent(&a.snapshot));
        if !all_quiescent {
            self.reset();
            return Verdict::Wait;
        }

        let current = digest(agents);
        match &self.last_digest {
            Some(prev) if *prev == current => {
                let since = *self.stable_since.get_or_insert(now);
                if now.saturating_sub(since) >= self.wave_margin {
                    Verdict::Retire
                } else {
                    Verdict::Wait
                }
            }
            _ => {
                self.last_digest = Some(current);
                self.stable_since = Some(now);
                Verdict::Wait
            }
        }
    }

    fn reset(&mut self) {
        self.stable_since = None;
        self.last_digest = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quiescent_snapshot() -> AgentSnapshot {
        AgentSnapshot {
            is_never: true,
            parked: true,
            tick: 1,
            ..AgentSnapshot::default()
        }
    }

    fn obs(id: &str, snapshot: AgentSnapshot) -> AgentObservation {
        AgentObservation {
            agent_id: id.into(),
            snapshot,
        }
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn quiescent_agent_matches_the_predicate() {
        assert!(agent_quiescent(&quiescent_snapshot()));
    }

    #[test]
    fn a_non_never_agent_is_never_quiescent() {
        let mut s = quiescent_snapshot();
        s.is_never = false;
        assert!(!agent_quiescent(&s));
    }

    #[test]
    fn a_mid_cycle_agent_with_empty_queues_is_not_quiescent() {
        let mut s = quiescent_snapshot();
        s.parked = false;
        assert!(!agent_quiescent(&s));
    }

    #[test]
    fn a_first_wake_agent_is_not_yet_quiescent() {
        let mut s = quiescent_snapshot();
        s.tick = 0;
        assert!(!agent_quiescent(&s));
    }

    #[test]
    fn a_pending_signal_defeats_quiescence() {
        let mut s = quiescent_snapshot();
        s.pending_triggers_count = 1;
        assert!(!agent_quiescent(&s));
    }

    #[test]
    fn retires_only_after_the_window_elapses() {
        let mut q = GraphQuiescence::new(secs(10));
        let agents = vec![
            obs("a", quiescent_snapshot()),
            obs("b", quiescent_snapshot()),
        ];
        assert_eq!(q.observe(secs(0), &agents), Verdict::Wait);
        assert_eq!(q.observe(secs(9), &agents), Verdict::Wait);
        assert_eq!(q.observe(secs(10), &agents), Verdict::Retire);
    }

    #[test]
    fn a_non_quiescent_sample_resets_the_clock() {
        let mut q = GraphQuiescence::new(secs(10));
        let quiet = vec![obs("a", quiescent_snapshot())];
        assert_eq!(q.observe(secs(0), &quiet), Verdict::Wait);

        let mut busy_snap = quiescent_snapshot();
        busy_snap.parked = false;
        let busy = vec![obs("a", busy_snap)];
        assert_eq!(q.observe(secs(9), &busy), Verdict::Wait);

        // The busy sample cleared the clock; the full window must re-elapse from
        // the next quiet sample (t=18), so eligibility is at 18 + 10 = 28.
        assert_eq!(q.observe(secs(18), &quiet), Verdict::Wait);
        assert_eq!(q.observe(secs(27), &quiet), Verdict::Wait);
        assert_eq!(q.observe(secs(28), &quiet), Verdict::Retire);
    }

    #[test]
    fn an_in_flight_signal_landing_mid_window_resets_the_clock() {
        // Both agents look quiescent, but a `ChildOutput` in flight to `b` lands
        // between samples: `b`'s cumulative counter bumps while it briefly wakes
        // and re-parks. The counter change must reset the window even though
        // every sample reads all-parked. This is the wave-window's whole point.
        let mut q = GraphQuiescence::new(secs(10));

        let mut b0 = quiescent_snapshot();
        b0.cumulative_triggers_observed = 5;
        let before = vec![obs("a", quiescent_snapshot()), obs("b", b0)];
        assert_eq!(q.observe(secs(0), &before), Verdict::Wait);

        let mut b1 = quiescent_snapshot();
        b1.cumulative_triggers_observed = 6;
        let after = vec![obs("a", quiescent_snapshot()), obs("b", b1)];
        assert_eq!(q.observe(secs(9), &after), Verdict::Wait);

        // Window restarted at the bump; not eligible until 9 + 10.
        assert_eq!(q.observe(secs(18), &after), Verdict::Wait);
        assert_eq!(q.observe(secs(19), &after), Verdict::Retire);
    }

    #[test]
    fn a_membership_change_resets_the_clock() {
        let mut q = GraphQuiescence::new(secs(10));
        let two = vec![
            obs("a", quiescent_snapshot()),
            obs("b", quiescent_snapshot()),
        ];
        assert_eq!(q.observe(secs(0), &two), Verdict::Wait);

        let three = vec![
            obs("a", quiescent_snapshot()),
            obs("b", quiescent_snapshot()),
            obs("c", quiescent_snapshot()),
        ];
        assert_eq!(q.observe(secs(9), &three), Verdict::Wait);
        assert_eq!(q.observe(secs(19), &three), Verdict::Retire);
    }

    #[test]
    fn an_empty_graph_is_not_retire_eligible() {
        let mut q = GraphQuiescence::new(secs(10));
        assert_eq!(q.observe(secs(0), &[]), Verdict::Wait);
        assert_eq!(q.observe(secs(100), &[]), Verdict::Wait);
    }
}
