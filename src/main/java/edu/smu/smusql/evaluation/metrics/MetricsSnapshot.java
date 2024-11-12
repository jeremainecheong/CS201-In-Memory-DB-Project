package edu.smu.smusql.evaluation.metrics;

/**
 * Holds metrics for all phases of a single test run
 */
public class MetricsSnapshot {
    public PhaseMetrics populationPhase;
    public PhaseMetrics mixedOperationsPhase;
    public PhaseMetrics complexQueriesPhase;
}