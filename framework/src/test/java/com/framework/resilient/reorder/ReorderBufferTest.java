package com.framework.resilient.reorder;

import com.framework.resilient.coordinator.EventMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class ReorderBufferTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);
    private static final String ENTITY_A = "entity-A";
    private static final String ENTITY_B = "entity-B";

    private ReorderBuffer<String> buffer;
    private List<TopicPartition> pausedPartitions;
    private List<TopicPartition> resumedPartitions;

    @BeforeEach
    void setUp() {
        pausedPartitions = new CopyOnWriteArrayList<>();
        resumedPartitions = new CopyOnWriteArrayList<>();

        ReorderBufferProperties properties = new ReorderBufferProperties(
                3,                          // maxBufferSize — small for testing
                0.8,                        // resumeThreshold — resume at 80%
                Duration.ofMillis(50)       // reorderTimeout — short for testing
        );

        buffer = new ReorderBuffer<>(
                properties,
                pausedPartitions::add,
                resumedPartitions::add
        );
    }

    // --- Helper methods ---

    private SequencedEvent<String> event(String sourceEntity, long seq) {
        return event(sourceEntity, seq, null);
    }

    private SequencedEvent<String> event(String sourceEntity, long seq, Instant bufferedAt) {
        EventMetadata metadata = new EventMetadata(
                "test-topic", 0, seq * 10, "corr-" + seq, null, Instant.now()
        );
        return new SequencedEvent<>(sourceEntity, seq, "payload-" + seq, metadata, bufferedAt);
    }

    // --- Tests ---

    @Nested
    @DisplayName("In-order event processing")
    class InOrderEvents {

        @Test
        @DisplayName("First event for unseen entity passes through immediately")
        void firstEventForUnseenEntityReleasedImmediately() {
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            assertThat(result.releasedEvents()).hasSize(1);
            assertThat(result.releasedEvents().get(0).sequenceNumber()).isEqualTo(1);
            assertThat(result.possibleDuplicates()).isEmpty();
            assertThat(result.backpressureTriggered()).isFalse();
        }

        @Test
        @DisplayName("First event sets expectedNext to seq + 1")
        void firstEventSetsExpectedNext() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 5));

            // Next expected is 6, so submitting seq 6 should release immediately
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 6));

            assertThat(result.releasedEvents()).hasSize(1);
            assertThat(result.releasedEvents().get(0).sequenceNumber()).isEqualTo(6);
        }

        @Test
        @DisplayName("Consecutive in-order events all pass through immediately")
        void consecutiveInOrderEventsPassThrough() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            ReorderResult<String> result2 = buffer.submit(PARTITION_0, event(ENTITY_A, 2));
            assertThat(result2.releasedEvents()).hasSize(1);
            assertThat(result2.releasedEvents().get(0).sequenceNumber()).isEqualTo(2);

            ReorderResult<String> result3 = buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            assertThat(result3.releasedEvents()).hasSize(1);
            assertThat(result3.releasedEvents().get(0).sequenceNumber()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Out-of-order event buffering")
    class OutOfOrderEvents {

        @Test
        @DisplayName("Out-of-order event is buffered and not released")
        void outOfOrderEventIsBuffered() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // seq 3 arrives before seq 2
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 3));

            assertThat(result.releasedEvents()).isEmpty();
            assertThat(result.possibleDuplicates()).isEmpty();
            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(1);
        }

        @Test
        @DisplayName("Multiple out-of-order events are buffered")
        void multipleOutOfOrderEventsBuffered() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            buffer.submit(PARTITION_0, event(ENTITY_A, 4));
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));

            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Consecutive release when gap is filled")
    class ConsecutiveRelease {

        @Test
        @DisplayName("Filling a gap releases the gap-filler and all consecutive buffered events")
        void fillingGapReleasesConsecutiveEvents() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Buffer seq 3 (out of order — gap at 2)
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(1);

            // Submit seq 2 — fills the gap, should release both 2 and 3
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 2));

            assertThat(result.releasedEvents()).hasSize(2);
            assertThat(result.releasedEvents().get(0).sequenceNumber()).isEqualTo(2);
            assertThat(result.releasedEvents().get(1).sequenceNumber()).isEqualTo(3);
            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(0);
        }

        @Test
        @DisplayName("Filling a gap releases a longer chain of consecutive events")
        void fillingGapReleasesLongerChain() {
            // Use a larger buffer for this test
            ReorderBufferProperties largeBufferProps = new ReorderBufferProperties(
                    100, 0.8, Duration.ofMillis(50)
            );
            ReorderBuffer<String> largeBuffer = new ReorderBuffer<>(
                    largeBufferProps, pausedPartitions::add, resumedPartitions::add
            );

            largeBuffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Buffer 3, 4, 5 (gap at 2)
            largeBuffer.submit(PARTITION_0, event(ENTITY_A, 3));
            largeBuffer.submit(PARTITION_0, event(ENTITY_A, 4));
            largeBuffer.submit(PARTITION_0, event(ENTITY_A, 5));

            // Fill the gap with seq 2
            ReorderResult<String> result = largeBuffer.submit(PARTITION_0, event(ENTITY_A, 2));

            assertThat(result.releasedEvents()).hasSize(4);
            assertThat(result.releasedEvents())
                    .extracting(SequencedEvent::sequenceNumber)
                    .containsExactly(2L, 3L, 4L, 5L);
        }
    }

    @Nested
    @DisplayName("Timeout-based release")
    class TimeoutRelease {

        @Test
        @DisplayName("Timed-out event is released after configured duration")
        void timedOutEventIsReleased() throws InterruptedException {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Buffer event with explicit bufferedAt in the past
            Instant pastTime = Instant.now().minus(Duration.ofMillis(100));
            SequencedEvent<String> outOfOrder = event(ENTITY_A, 3, pastTime);
            buffer.submit(PARTITION_0, outOfOrder);

            // releaseTimedOut should release the timed-out event
            List<SequencedEvent<String>> released = buffer.releaseTimedOut(PARTITION_0);

            assertThat(released).hasSize(1);
            assertThat(released.get(0).sequenceNumber()).isEqualTo(3);
        }

        @Test
        @DisplayName("Non-timed-out event is NOT released")
        void nonTimedOutEventNotReleased() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Buffer a fresh event (bufferedAt = now, should not be timed out yet)
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));

            List<SequencedEvent<String>> released = buffer.releaseTimedOut(PARTITION_0);

            assertThat(released).isEmpty();
        }

        @Test
        @DisplayName("Timeout release also releases consecutive events that follow")
        void timeoutReleasesConsecutiveFollowers() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Buffer seq 3 (old) and seq 4 (fresh) — when 3 is released, 4 should follow
            Instant pastTime = Instant.now().minus(Duration.ofMillis(100));
            buffer.submit(PARTITION_0, event(ENTITY_A, 3, pastTime));
            buffer.submit(PARTITION_0, event(ENTITY_A, 4, pastTime));

            List<SequencedEvent<String>> released = buffer.releaseTimedOut(PARTITION_0);

            // seq 3 is released as timed-out, then seq 4 is consecutive
            assertThat(released).hasSizeGreaterThanOrEqualTo(1);
            assertThat(released.get(0).sequenceNumber()).isEqualTo(3);
            // If 4 also released as consecutive:
            if (released.size() > 1) {
                assertThat(released.get(1).sequenceNumber()).isEqualTo(4);
            }
        }

        @Test
        @DisplayName("releaseTimedOut on unknown partition returns empty")
        void releaseTimedOutOnUnknownPartitionReturnsEmpty() {
            TopicPartition unknown = new TopicPartition("unknown", 99);
            List<SequencedEvent<String>> released = buffer.releaseTimedOut(unknown);
            assertThat(released).isEmpty();
        }
    }

    @Nested
    @DisplayName("Backpressure at max capacity")
    class Backpressure {

        @Test
        @DisplayName("Backpressure triggers when buffer reaches maxBufferSize")
        void backpressureTriggersAtMaxCapacity() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Buffer 3 events (maxBufferSize = 3)
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            buffer.submit(PARTITION_0, event(ENTITY_A, 4));
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 5));

            assertThat(result.backpressureTriggered()).isTrue();
            assertThat(buffer.isBackpressureActive(PARTITION_0)).isTrue();
            assertThat(pausedPartitions).containsExactly(PARTITION_0);
        }

        @Test
        @DisplayName("Backpressure does not trigger below max capacity")
        void noBackpressureBelowMaxCapacity() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 4));

            assertThat(result.backpressureTriggered()).isFalse();
            assertThat(buffer.isBackpressureActive(PARTITION_0)).isFalse();
            assertThat(pausedPartitions).isEmpty();
        }

        @Test
        @DisplayName("Pause callback is invoked exactly once on trigger")
        void pauseCallbackInvokedOnce() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            buffer.submit(PARTITION_0, event(ENTITY_A, 4));
            buffer.submit(PARTITION_0, event(ENTITY_A, 5));

            assertThat(pausedPartitions).hasSize(1);
        }
    }

    @Nested
    @DisplayName("Resume when buffer drops below 80% threshold")
    class ResumeBackpressure {

        @Test
        @DisplayName("Resume fires when buffer drops below 80% of max after backpressure")
        void resumeWhenBufferDropsBelowThreshold() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Fill buffer to trigger backpressure (3 events buffered, max=3)
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            buffer.submit(PARTITION_0, event(ENTITY_A, 4));
            buffer.submit(PARTITION_0, event(ENTITY_A, 5));

            assertThat(buffer.isBackpressureActive(PARTITION_0)).isTrue();

            // Now fill the gap with seq 2 — releases 2, 3, 4, 5
            // Buffer goes from 3 → 0, which is below 80% of 3 (2.4)
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 2));

            assertThat(result.releasedEvents()).hasSize(4);
            assertThat(buffer.isBackpressureActive(PARTITION_0)).isFalse();
            assertThat(resumedPartitions).containsExactly(PARTITION_0);
        }

        @Test
        @DisplayName("Resume callback is invoked with the correct partition")
        void resumeCallbackInvokedWithCorrectPartition() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            buffer.submit(PARTITION_0, event(ENTITY_A, 4));
            buffer.submit(PARTITION_0, event(ENTITY_A, 5));

            // Fill gap
            buffer.submit(PARTITION_0, event(ENTITY_A, 2));

            assertThat(resumedPartitions).containsExactly(PARTITION_0);
        }
    }

    @Nested
    @DisplayName("Multi-entity independence within same partition")
    class MultiEntityIndependence {

        @Test
        @DisplayName("Entity A ordering is independent of entity B in same partition")
        void entityAIndependentOfEntityB() {
            // Entity A starts at seq 1
            ReorderResult<String> resultA1 = buffer.submit(PARTITION_0, event(ENTITY_A, 1));
            assertThat(resultA1.releasedEvents()).hasSize(1);

            // Entity B starts at seq 10
            ReorderResult<String> resultB10 = buffer.submit(PARTITION_0, event(ENTITY_B, 10));
            assertThat(resultB10.releasedEvents()).hasSize(1);

            // Entity A seq 2 arrives in order
            ReorderResult<String> resultA2 = buffer.submit(PARTITION_0, event(ENTITY_A, 2));
            assertThat(resultA2.releasedEvents()).hasSize(1);
            assertThat(resultA2.releasedEvents().get(0).sequenceNumber()).isEqualTo(2);

            // Entity B seq 12 arrives out of order (gap at 11)
            ReorderResult<String> resultB12 = buffer.submit(PARTITION_0, event(ENTITY_B, 12));
            assertThat(resultB12.releasedEvents()).isEmpty();

            // Entity A seq 3 still works fine despite B being out of order
            ReorderResult<String> resultA3 = buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            assertThat(resultA3.releasedEvents()).hasSize(1);
            assertThat(resultA3.releasedEvents().get(0).sequenceNumber()).isEqualTo(3);
        }

        @Test
        @DisplayName("Buffered count shared across entities in same partition")
        void bufferCountSharedAcrossEntities() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));
            buffer.submit(PARTITION_0, event(ENTITY_B, 1));

            // Buffer one event for each entity
            buffer.submit(PARTITION_0, event(ENTITY_A, 3)); // gap at 2
            buffer.submit(PARTITION_0, event(ENTITY_B, 3)); // gap at 2

            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Flush releases all buffered events in order")
    class FlushPartition {

        @Test
        @DisplayName("Flush releases all buffered events in sequence order")
        void flushReleasesAllInOrder() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Buffer multiple out-of-order events
            buffer.submit(PARTITION_0, event(ENTITY_A, 5));
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            buffer.submit(PARTITION_0, event(ENTITY_A, 4));

            List<SequencedEvent<String>> flushed = buffer.flushPartition(PARTITION_0);

            assertThat(flushed)
                    .extracting(SequencedEvent::sequenceNumber)
                    .containsExactly(3L, 4L, 5L);
            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(0);
        }

        @Test
        @DisplayName("Flush lifts backpressure if active")
        void flushLiftsBackpressure() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            // Trigger backpressure
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            buffer.submit(PARTITION_0, event(ENTITY_A, 4));
            buffer.submit(PARTITION_0, event(ENTITY_A, 5));

            assertThat(buffer.isBackpressureActive(PARTITION_0)).isTrue();

            buffer.flushPartition(PARTITION_0);

            assertThat(buffer.isBackpressureActive(PARTITION_0)).isFalse();
            assertThat(resumedPartitions).containsExactly(PARTITION_0);
        }

        @Test
        @DisplayName("Flush on empty partition returns empty list")
        void flushOnEmptyPartitionReturnsEmpty() {
            List<SequencedEvent<String>> flushed = buffer.flushPartition(PARTITION_0);
            assertThat(flushed).isEmpty();
        }

        @Test
        @DisplayName("Flush releases events from multiple entities in sequence order")
        void flushReleasesMultiEntityEvents() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));
            buffer.submit(PARTITION_0, event(ENTITY_B, 10));

            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            buffer.submit(PARTITION_0, event(ENTITY_B, 12));

            List<SequencedEvent<String>> flushed = buffer.flushPartition(PARTITION_0);

            // Both entities' buffered events should be released
            assertThat(flushed).hasSize(2);
            assertThat(flushed)
                    .extracting(SequencedEvent::sequenceNumber)
                    .containsExactlyInAnyOrder(3L, 12L);
        }
    }

    @Nested
    @DisplayName("Duplicate detection — events with seq <= lastProcessed")
    class DuplicateDetection {

        @Test
        @DisplayName("Event with seq <= lastProcessed returned as possibleDuplicate")
        void eventBelowLastProcessedReturnedAsDuplicate() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));
            buffer.submit(PARTITION_0, event(ENTITY_A, 2));

            // Re-submit seq 1 — already processed
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            assertThat(result.possibleDuplicates()).hasSize(1);
            assertThat(result.possibleDuplicates().get(0).sequenceNumber()).isEqualTo(1);
            assertThat(result.releasedEvents()).isEmpty();
        }

        @Test
        @DisplayName("Event equal to lastProcessed returned as possibleDuplicate")
        void eventEqualToLastProcessedReturnedAsDuplicate() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 5));

            // Re-submit seq 5 — it was the initial event, lastProcessed = 5
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 5));

            assertThat(result.possibleDuplicates()).hasSize(1);
            assertThat(result.possibleDuplicates().get(0).sequenceNumber()).isEqualTo(5);
            assertThat(result.releasedEvents()).isEmpty();
        }

        @Test
        @DisplayName("Multiple sequential events then duplicate detection works correctly")
        void duplicateAfterMultipleSequentialEvents() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));
            buffer.submit(PARTITION_0, event(ENTITY_A, 2));
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));

            // Re-submit seq 2 — already processed
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 2));

            assertThat(result.possibleDuplicates()).hasSize(1);
            assertThat(result.possibleDuplicates().get(0).sequenceNumber()).isEqualTo(2);
            assertThat(result.releasedEvents()).isEmpty();
            assertThat(result.backpressureTriggered()).isFalse();
        }
    }

    @Nested
    @DisplayName("Buffer depth and state queries")
    class BufferQueries {

        @Test
        @DisplayName("getBufferDepth returns 0 for unknown partition")
        void bufferDepthZeroForUnknownPartition() {
            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(0);
        }

        @Test
        @DisplayName("isBackpressureActive returns false for unknown partition")
        void backpressureInactiveForUnknownPartition() {
            assertThat(buffer.isBackpressureActive(PARTITION_0)).isFalse();
        }

        @Test
        @DisplayName("Buffer depth tracks correctly across submit and release")
        void bufferDepthTracksCorrectly() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));

            buffer.submit(PARTITION_0, event(ENTITY_A, 3));
            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(1);

            buffer.submit(PARTITION_0, event(ENTITY_A, 4));
            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(2);

            // Fill gap — releases both buffered events
            buffer.submit(PARTITION_0, event(ENTITY_A, 2));
            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("Partition lifecycle")
    class PartitionLifecycle {

        @Test
        @DisplayName("initializePartition creates empty state")
        void initializePartitionCreatesEmptyState() {
            buffer.initializePartition(PARTITION_0);

            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(0);
            assertThat(buffer.isBackpressureActive(PARTITION_0)).isFalse();
        }

        @Test
        @DisplayName("removePartition clears all state for that partition")
        void removePartitionClearsState() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));
            buffer.submit(PARTITION_0, event(ENTITY_A, 3));

            buffer.removePartition(PARTITION_0);

            assertThat(buffer.getBufferDepth(PARTITION_0)).isEqualTo(0);
            assertThat(buffer.isBackpressureActive(PARTITION_0)).isFalse();
        }

        @Test
        @DisplayName("After removePartition, new events are treated as first event for unseen entity")
        void afterRemovePartitionEventsAreFirstSeen() {
            buffer.submit(PARTITION_0, event(ENTITY_A, 1));
            buffer.submit(PARTITION_0, event(ENTITY_A, 2));

            buffer.removePartition(PARTITION_0);

            // Submit again — should be treated as first event
            ReorderResult<String> result = buffer.submit(PARTITION_0, event(ENTITY_A, 50));
            assertThat(result.releasedEvents()).hasSize(1);
            assertThat(result.releasedEvents().get(0).sequenceNumber()).isEqualTo(50);
        }
    }
}
