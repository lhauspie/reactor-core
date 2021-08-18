/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/**
 * A {@link Flux} API sub-group that exposes all the buffering operators. Exposed via {@link Flux#buffers()}.
 *
 * @author Simon Baslé
 */
public final class FluxApiGroupBuffer<T> {

	private final Flux<T> source;

	FluxApiGroupBuffer(Flux<T> source) {
		this.source = source;
	}

	/**
	 * Collect all incoming values into a single {@link List} buffer that will be emitted
	 * by the returned {@link Flux} once this Flux completes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/buffer.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the buffer upon cancellation or error triggered by a data signal.
	 *
	 * @return a buffered {@link Flux} of at most one {@link List}
	 *
	 * @see Flux#collectList() for an alternative collecting algorithm returning {@link Mono}
	 */
	public Flux<List<T>> all() {
		return bySize(Integer.MAX_VALUE);
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the given max size is reached or once this
	 * Flux completes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum collected size
	 *
	 * @return a buffered {@link Flux} of {@link List}
	 */
	public Flux<List<T>> bySize(int maxSize) {
		return bySize(maxSize, Flux.listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the given max size is reached
	 * or once this Flux completes.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal,
	 * as well as latest unbuffered element if the bufferSupplier fails.
	 *
	 * @param maxSize the maximum collected size
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a buffered {@link Flux} of {@link Collection}
	 */
	public <C extends Collection<? super T>> Flux<C> bySize(int maxSize, Supplier<C> bufferSupplier) {
		return Flux.onAssembly(new FluxBuffer<>(this.source, maxSize, bufferSupplier));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the buffer reaches a maximum size OR the
	 * maxTime {@link Duration} elapses.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 *
	 * @return a buffered {@link Flux} of {@link List} delimited by given size or a given period timeout
	 */
	public Flux<List<T>> bySizeOrTimeout(int maxSize, Duration maxTime) {
		return bySizeOrTimeout(maxSize, maxTime, Flux.listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the buffer reaches a maximum
	 * size OR the maxTime {@link Duration} elapses.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a buffered {@link Flux} of {@link Collection} delimited by given size or a given period timeout
	 */
	public <C extends Collection<? super T>> Flux<C> bySizeOrTimeout(int maxSize, Duration maxTime,
																	 Supplier<C> bufferSupplier) {
		return bySizeOrTimeout(maxSize, maxTime, Schedulers.parallel(),
			bufferSupplier);
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the buffer reaches a maximum size OR the
	 * maxTime {@link Duration} elapses, as measured on the provided {@link Scheduler}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a buffered {@link Flux} of {@link List} delimited by given size or a given period timeout
	 */
	public Flux<List<T>> bySizeOrTimeout(int maxSize, Duration maxTime, Scheduler timer) {
		return bySizeOrTimeout(maxSize, maxTime, timer, Flux.listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the buffer reaches a maximum
	 * size OR the maxTime {@link Duration} elapses, as measured on the provided {@link Scheduler}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferTimeoutWithMaxSizeAndTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the max collected size
	 * @param maxTime the timeout enforcing the release of a partial buffer
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a buffered {@link Flux} of {@link Collection} delimited by given size or a given period timeout
	 */
	public <C extends Collection<? super T>> Flux<C> bySizeOrTimeout(int maxSize, Duration maxTime,
																	 Scheduler timer,
																	 Supplier<C> bufferSupplier) {
		return Flux.onAssembly(new FluxBufferTimeout<>(this.source, maxSize, maxTime.toNanos(), TimeUnit.NANOSECONDS, timer, bufferSupplier));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted
	 * by the returned {@link Flux} each time the given max size is reached or once this
	 * Flux completes. Buffers can be created with gaps, as a new buffer will be created
	 * every time {@code skip} values have been emitted by the source.
	 * <p>
	 * When maxSize < skip : dropping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeLessThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize > skip : overlapping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeGreaterThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize == skip : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeEqualsSkipSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements in between buffers (in the case of
	 * dropping buffers). It also discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * Note however that overlapping buffer variant DOES NOT discard, as this might result in an element
	 * being discarded from an early buffer while it is still valid in a more recent buffer.
	 *
	 * @param skip the number of items to count before creating a new buffer
	 * @param maxSize the max collected size
	 *
	 * @return a buffered {@link Flux} of possibly overlapped or gapped {@link List}
	 */
	public Flux<List<T>> bySizeWithSkip(int maxSize, int skip) {
		return bySizeWithSkip(maxSize, skip, Flux.listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers that
	 * will be emitted by the returned {@link Flux} each time the given max size is reached
	 * or once this Flux completes. Buffers can be created with gaps, as a new buffer will
	 * be created every time {@code skip} values have been emitted by the source
	 * <p>
	 * When maxSize < skip : dropping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeLessThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize > skip : overlapping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeGreaterThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize == skip : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithMaxSizeEqualsSkipSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements in between buffers (in the case of
	 * dropping buffers). It also discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * Note however that overlapping buffer variant DOES NOT discard, as this might result in an element
	 * being discarded from an early buffer while it is still valid in a more recent buffer.
	 *
	 * @param skip the number of items to count before creating a new buffer
	 * @param maxSize the max collected size
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a buffered {@link Flux} of possibly overlapped or gapped
	 * {@link Collection}
	 */
	public <C extends Collection<? super T>> Flux<C> bySizeWithSkip(int maxSize, int skip,
																	Supplier<C> bufferSupplier) {
		return Flux.onAssembly(new FluxBuffer<>(this.source, maxSize, skip, bufferSupplier));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the returned {@link Flux} every {@code bufferingTimespan}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param bufferingTimespan the duration from buffer creation until a buffer is closed and emitted
	 *
	 * @return a buffered {@link Flux} of {@link List} delimited by the given time span
	 */
	public Flux<List<T>> byTime(Duration bufferingTimespan) {
		return byTime(bufferingTimespan, Schedulers.parallel());
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the returned {@link Flux} every {@code bufferingTimespan}, as measured on the provided {@link Scheduler}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param bufferingTimespan the duration from buffer creation until a buffer is closed and emitted
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a buffered {@link Flux} of {@link List} delimited by the given period
	 */
	public Flux<List<T>> byTime(Duration bufferingTimespan, Scheduler timer) {
		return splitWhen(Flux.interval(bufferingTimespan, timer));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers created at a given
	 * {@code openBufferEvery} period. Each buffer will last until the {@code bufferingTimespan} has elapsed,
	 * thus emitting the bucket in the resulting {@link Flux}.
	 * <p>
	 * When bufferingTimespan < openBufferEvery : dropping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanLessThanOpenBufferEvery.svg" alt="">
	 * <p>
	 * When bufferingTimespan > openBufferEvery : overlapping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanGreaterThanOpenBufferEvery.svg" alt="">
	 * <p>
	 * When bufferingTimespan == openBufferEvery : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanEqualsOpenBufferEvery.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * It DOES NOT provide strong guarantees in the case of overlapping buffers, as elements
	 * might get discarded too early (from the first of two overlapping buffers for instance).
	 *
	 * @param bufferingTimespan the duration from buffer creation until a buffer is closed and emitted
	 * @param openBufferEvery the interval at which to create a new buffer
	 *
	 * @return a buffered {@link Flux} of {@link List} delimited by the given period openBufferEvery and sized by bufferingTimespan
	 */
	public Flux<List<T>> byTimeWithSkip(Duration bufferingTimespan, Duration openBufferEvery) {
		return byTimeWithSkip(bufferingTimespan, openBufferEvery, Schedulers.parallel());
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers created at a given
	 * {@code openBufferEvery} period, as measured on the provided {@link Scheduler}. Each
	 * buffer will last until the {@code bufferingTimespan} has elapsed (also measured on the scheduler),
	 * thus emitting the bucket in the resulting {@link Flux}.
	 * <p>
	 * When bufferingTimespan < openBufferEvery : dropping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanLessThanOpenBufferEvery.svg" alt="">
	 * <p>
	 * When bufferingTimespan > openBufferEvery : overlapping buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanGreaterThanOpenBufferEvery.svg" alt="">
	 * <p>
	 * When bufferingTimespan == openBufferEvery : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithTimespanEqualsOpenBufferEvery.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * It DOES NOT provide strong guarantees in the case of overlapping buffers, as elements
	 * might get discarded too early (from the first of two overlapping buffers for instance).
	 *
	 * @param bufferingTimespan the duration from buffer creation until a buffer is closed and emitted
	 * @param openBufferEvery the interval at which to create a new buffer
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a buffered {@link Flux} of {@link List} delimited by the given period openBufferEvery and sized by bufferingTimespan
	 */
	public Flux<List<T>> byTimeWithSkip(Duration bufferingTimespan, Duration openBufferEvery, Scheduler timer) {
		if (bufferingTimespan.equals(openBufferEvery)) {
			return byTime(bufferingTimespan, timer);
		}
		return when(Flux.interval(Duration.ZERO, openBufferEvery, timer), aLong -> Mono
			.delay(bufferingTimespan, timer));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the resulting {@link Flux}. Each buffer continues aggregating values while the
	 * given predicate returns true, and a new buffer is created as soon as the
	 * predicate returns false... Note that the element that triggers the predicate
	 * to return false (and thus closes a buffer) is NOT included in any emitted buffer.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWhile.svg" alt="">
	 * <p>
	 * On completion, if the latest buffer is non-empty and has not been closed it is
	 * emitted. However, such a "partial" buffer isn't emitted in case of onError
	 * termination.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal,
	 * as well as the buffer-triggering element.
	 *
	 * @param predicate a predicate that triggers the next buffer when it becomes false.
	 *
	 * @return a buffered {@link Flux} of {@link List}
	 */
	public Flux<List<T>> splitIf(Predicate<? super T> predicate) {
		return Flux.onAssembly(new FluxBufferPredicate<>(this.source, predicate,
			Flux.listSupplier(), FluxBufferPredicate.Mode.WHILE));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers, as delimited by the
	 * signals of a companion {@link Publisher} this operator will subscribe to.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithBoundary.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param other the companion {@link Publisher} whose signals trigger new buffers
	 *
	 * @return a buffered {@link Flux} of {@link List} delimited by signals from a {@link Publisher}
	 */
	public Flux<List<T>> splitWhen(Publisher<?> other) {
		return splitWhen(other, Flux.listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers, as
	 * delimited by the signals of a companion {@link Publisher} this operator will
	 * subscribe to.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWithBoundary.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal,
	 * and the last received element when the bufferSupplier fails.
	 *
	 * @param other the companion {@link Publisher} whose signals trigger new buffers
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a buffered {@link Flux} of {@link Collection} delimited by signals from a {@link Publisher}
	 */
	public <C extends Collection<? super T>> Flux<C> splitWhen(Publisher<?> other, Supplier<C> bufferSupplier) {
		return Flux.onAssembly(new FluxBufferBoundary<>(this.source, other, bufferSupplier));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the resulting {@link Flux} each time the given predicate returns true. Note that
	 * the element that triggers the predicate to return true (and thus closes a buffer)
	 * is included as last element in the emitted buffer.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntil.svg" alt="">
	 * <p>
	 * On completion, if the latest buffer is non-empty and has not been closed it is
	 * emitted. However, such a "partial" buffer isn't emitted in case of onError
	 * termination.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param predicate a predicate that triggers the next buffer when it becomes true.
	 *
	 * @return a buffered {@link Flux} of {@link List}
	 */
	public Flux<List<T>> until(Predicate<? super T> predicate) {
		return Flux.onAssembly(new FluxBufferPredicate<>(this.source, predicate,
			Flux.listSupplier(), FluxBufferPredicate.Mode.UNTIL));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers that will be emitted by
	 * the resulting {@link Flux} each time the given predicate returns true. Note that
	 * the buffer into which the element that triggers the predicate to return true
	 * (and thus closes a buffer) is included depends on the {@code cutBefore} parameter:
	 * set it to true to include the boundary element in the newly opened buffer, false to
	 * include it in the closed buffer (as in {@link #until(Predicate)}).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntilWithCutBefore.svg" alt="">
	 * <p>
	 * On completion, if the latest buffer is non-empty and has not been closed it is
	 * emitted. However, such a "partial" buffer isn't emitted in case of onError
	 * termination.
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 *
	 * @param predicate a predicate that triggers the next buffer when it becomes true.
	 * @param cutBefore set to true to include the triggering element in the new buffer rather than the old.
	 *
	 * @return a buffered {@link Flux} of {@link List}
	 */
	public Flux<List<T>> until(Predicate<? super T> predicate, boolean cutBefore) {
		return Flux.onAssembly(new FluxBufferPredicate<>(this.source, predicate, Flux.listSupplier(),
			cutBefore ? FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE
				: FluxBufferPredicate.Mode.UNTIL));
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another) into multiple {@link List} buffers that will be emitted by the
	 * resulting {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntilChanged.svg" alt="">
	 * <p>
	 *
	 * @return a buffered {@link Flux} of {@link List}
	 */
	public <V> Flux<List<T>> untilChanged() {
		return untilChanged(Flux.identityFunction());
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link
	 * Function}, into multiple {@link List} buffers that will be emitted by the
	 * resulting {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntilChangedWithKey.svg" alt="">
	 * <p>
	 *
	 * @param keySelector function to compute comparison key for each element
	 *
	 * @return a buffered {@link Flux} of {@link List}
	 */
	public <V> Flux<List<T>> untilChanged(Function<? super T, ? extends V> keySelector) {
		return untilChanged(keySelector, Flux.equalPredicate());
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link
	 * Function} and compared using a supplied {@link BiPredicate}, into multiple
	 * {@link List} buffers that will be emitted by the resulting {@link Flux}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferUntilChangedWithKey.svg" alt="">
	 * <p>
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param keyComparator predicate used to compare keys
	 *
	 * @return a buffered {@link Flux} of {@link List}
	 */
	public <V> Flux<List<T>> untilChanged(Function<? super T, ? extends V> keySelector,
										  BiPredicate<? super V, ? super V> keyComparator) {
		return Flux.defer(() -> until(new FluxBufferPredicate.ChangedPredicate<T, V>(keySelector,
			keyComparator), true));
	}

	/**
	 * Collect incoming values into multiple {@link List} buffers started each time an opening
	 * companion {@link Publisher} emits. Each buffer will last until the corresponding
	 * closing companion {@link Publisher} emits, thus releasing the buffer to the resulting {@link Flux}.
	 * <p>
	 * When Open signal is strictly not overlapping Close signal : dropping buffers (see green marbles in diagram below).
	 * <p>
	 * When Open signal is strictly more frequent than Close signal : overlapping buffers (see second and third buffers in diagram below).
	 * <p>
	 * When Open signal is exactly coordinated with Close signal : exact buffers
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWhen.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * It DOES NOT provide strong guarantees in the case of overlapping buffers, as elements
	 * might get discarded too early (from the first of two overlapping buffers for instance).
	 *
	 * @param bucketOpening a companion {@link Publisher} to subscribe for buffer creation signals.
	 * @param closeSelector a factory that, given a buffer opening signal, returns a companion
	 * {@link Publisher} to subscribe to for buffer closure and emission signals.
	 * @param <U> the element type of the buffer-opening sequence
	 * @param <V> the element type of the buffer-closing sequence
	 *
	 * @return a buffered {@link Flux} of {@link List} delimited by an opening {@link Publisher} and a relative
	 * closing {@link Publisher}
	 */
	public <U, V> Flux<List<T>> when(Publisher<U> bucketOpening,
									 Function<? super U, ? extends Publisher<V>> closeSelector) {
		return when(bucketOpening, closeSelector, Flux.listSupplier());
	}

	/**
	 * Collect incoming values into multiple user-defined {@link Collection} buffers started each time an opening
	 * companion {@link Publisher} emits. Each buffer will last until the corresponding
	 * closing companion {@link Publisher} emits, thus releasing the buffer to the resulting {@link Flux}.
	 * <p>
	 * When Open signal is strictly not overlapping Close signal : dropping buffers (see green marbles in diagram below).
	 * <p>
	 * When Open signal is strictly more frequent than Close signal : overlapping buffers (see second and third buffers in diagram below).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/bufferWhenWithSupplier.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards the currently open buffer upon cancellation or error triggered by a data signal.
	 * It DOES NOT provide strong guarantees in the case of overlapping buffers, as elements
	 * might get discarded too early (from the first of two overlapping buffers for instance).
	 *
	 * @param bucketOpening a companion {@link Publisher} to subscribe for buffer creation signals.
	 * @param closeSelector a factory that, given a buffer opening signal, returns a companion
	 * {@link Publisher} to subscribe to for buffer closure and emission signals.
	 * @param bufferSupplier a {@link Supplier} of the concrete {@link Collection} to use for each buffer
	 * @param <U> the element type of the buffer-opening sequence
	 * @param <V> the element type of the buffer-closing sequence
	 * @param <C> the {@link Collection} buffer type
	 *
	 * @return a buffered {@link Flux} of {@link Collection} delimited by an opening {@link Publisher} and a relative
	 * closing {@link Publisher}
	 */
	public <U, V, C extends Collection<? super T>> Flux<C> when(Publisher<U> bucketOpening,
																Function<? super U, ? extends Publisher<V>> closeSelector,
																Supplier<C> bufferSupplier) {
		return Flux.onAssembly(new FluxBufferWhen<>(this.source, bucketOpening, closeSelector,
			bufferSupplier, Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}
}
