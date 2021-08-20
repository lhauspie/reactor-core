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
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/**
 * A {@link Flux} API sub-group that exposes all the windowing operators. Exposed via {@link Flux#windows()}.
 *
 * @author Simon Baslé
 */
public final class FluxApiGroupWindow<T> {

	private final Flux<T> source;

	FluxApiGroupWindow(Flux<T> source) {
		this.source = source;
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows containing
	 * {@code maxSize} elements (or less for the final window) and starting from the first item.
	 * Each {@link Flux} window will onComplete after {@code maxSize} items have been routed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithMaxSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count
	 */
	public Flux<Flux<T>> bySize(int maxSize) {
		return Flux.onAssembly(new FluxWindow<>(this.source, maxSize, Queues.get(maxSize)));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows containing
	 * {@code maxSize} elements (or less for the final window) and starting from the first item.
	 * Each {@link Flux} window will onComplete once it contains {@code maxSize} elements
	 * OR it has been open for the given {@link Duration} (as measured on the {@link Schedulers#parallel() parallel}
	 * Scheduler).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowTimeout.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 * @param maxTime the maximum {@link Duration} since the window was opened before closing it
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count and duration
	 */
	public Flux<Flux<T>> bySizeOrTimeout(int maxSize, Duration maxTime) {
		return bySizeOrTimeout(maxSize, maxTime, Schedulers.parallel());
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows containing
	 * {@code maxSize} elements (or less for the final window) and starting from the first item.
	 * Each {@link Flux} window will onComplete once it contains {@code maxSize} elements
	 * OR it has been open for the given {@link Duration} (as measured on the provided
	 * {@link Scheduler}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowTimeout.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 * @param maxTime the maximum {@link Duration} since the window was opened before closing it
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count and duration
	 */
	public Flux<Flux<T>> bySizeOrTimeout(int maxSize, Duration maxTime, Scheduler timer) {
		return Flux.onAssembly(new FluxWindowTimeout<>(this.source, maxSize, maxTime.toNanos(), TimeUnit.NANOSECONDS, timer));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows of size
	 * {@code maxSize}, that each open every {@code skip} elements in the source.
	 *
	 * <p>
	 * When maxSize < skip : dropping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithMaxSizeLessThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize > skip : overlapping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithMaxSizeGreaterThanSkipSize.svg" alt="">
	 * <p>
	 * When maxSize == skip : exact windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithMaxSizeEqualsSkipSize.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> The overlapping variant DOES NOT discard elements, as they might be part of another still valid window.
	 * The exact window and dropping window variants bot discard elements they internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. The dropping window variant also discards elements in between windows.
	 *
	 * @param maxSize the maximum number of items to emit in the window before closing it
	 * @param skip the number of items to count before opening and emitting a new window
	 *
	 * @return a {@link Flux} of {@link Flux} windows based on element count and opened every skipCount
	 */
	public Flux<Flux<T>> bySizeWithSkip(int maxSize, int skip) {
		return Flux.onAssembly(new FluxWindow<>(this.source,
			maxSize,
			skip,
			Queues.unbounded(Queues.XS_BUFFER_SIZE),
			Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}

	/**
	 * Split this {@link Flux} sequence into continuous, non-overlapping windows that open
	 * for a {@code windowingTimespan} {@link Duration} (as measured on the {@link Schedulers#parallel() parallel}
	 * Scheduler).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param windowingTimespan the {@link Duration} to delimit {@link Flux} windows
	 *
	 * @return a {@link Flux} of {@link Flux} windows continuously opened for a given {@link Duration}
	 */
	public Flux<Flux<T>> byTime(Duration windowingTimespan) {
		return byTime(windowingTimespan, Schedulers.parallel());
	}

	/**
	 * Split this {@link Flux} sequence into continuous, non-overlapping windows that open
	 * for a {@code windowingTimespan} {@link Duration} (as measured on the provided {@link Scheduler}).
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespan.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param windowingTimespan the {@link Duration} to delimit {@link Flux} windows
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} of {@link Flux} windows continuously opened for a given {@link Duration}
	 */
	public Flux<Flux<T>> byTime(Duration windowingTimespan, Scheduler timer) {
		return splitWhen(Flux.interval(windowingTimespan, timer));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows that open
	 * for a given {@code windowingTimespan} {@link Duration}, after which it closes with onComplete.
	 * Each window is opened at a regular {@code timeShift} interval, starting from the
	 * first item.
	 * Both durations are measured on the {@link Schedulers#parallel() parallel} Scheduler.
	 *
	 * <p>
	 * When windowingTimespan < openWindowEvery : dropping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanLessThanOpenWindowEvery.svg" alt="">
	 * <p>
	 * When windowingTimespan > openWindowEvery : overlapping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanGreaterThanOpenWindowEvery.svg" alt="">
	 * <p>
	 * When windowingTimespan == openWindowEvery : exact windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanEqualsOpenWindowEvery.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> The overlapping variant DOES NOT discard elements, as they might be part of another still valid window.
	 * The exact window and dropping window variants bot discard elements they internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. The dropping window variant also discards elements in between windows.
	 *
	 * @param windowingTimespan the maximum {@link Flux} window {@link Duration}
	 * @param openWindowEvery the period of time at which to create new {@link Flux} windows
	 *
	 * @return a {@link Flux} of {@link Flux} windows opened at regular intervals and
	 * closed after a {@link Duration}
	 */
	public Flux<Flux<T>> byTimeWithSkip(Duration windowingTimespan, Duration openWindowEvery) {
		return byTimeWithSkip(windowingTimespan, openWindowEvery, Schedulers.parallel());
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows that open
	 * for a given {@code windowingTimespan} {@link Duration}, after which it closes with onComplete.
	 * Each window is opened at a regular {@code timeShift} interval, starting from the
	 * first item.
	 * Both durations are measured on the provided {@link Scheduler}.
	 *
	 * <p>
	 * When windowingTimespan < openWindowEvery : dropping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanLessThanOpenWindowEvery.svg" alt="">
	 * <p>
	 * When windowingTimespan > openWindowEvery : overlapping windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanGreaterThanOpenWindowEvery.svg" alt="">
	 * <p>
	 * When openWindowEvery == openWindowEvery : exact windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithTimespanEqualsOpenWindowEvery.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> The overlapping variant DOES NOT discard elements, as they might be part of another still valid window.
	 * The exact window and dropping window variants bot discard elements they internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. The dropping window variant also discards elements in between windows.
	 *
	 * @param windowingTimespan the maximum {@link Flux} window {@link Duration}
	 * @param openWindowEvery the period of time at which to create new {@link Flux} windows
	 * @param timer a time-capable {@link Scheduler} instance to run on
	 *
	 * @return a {@link Flux} of {@link Flux} windows opened at regular intervals and
	 * closed after a {@link Duration}
	 */
	public Flux<Flux<T>> byTimeWithSkip(Duration windowingTimespan, Duration openWindowEvery, Scheduler timer) {
		if (openWindowEvery.equals(windowingTimespan)) {
			return byTime(windowingTimespan);
		}
		return when(Flux.interval(Duration.ZERO, openWindowEvery, timer), aLong -> Mono.delay(windowingTimespan, timer));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows delimited by the
	 * given predicate. A new window is opened each time the predicate returns true, at which
	 * point the previous window will receive the triggering element then onComplete.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window errors). This variant shouldn't
	 * expose empty windows, as the separators are emitted into
	 * the windows they close.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntil.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param boundaryTrigger a predicate that triggers the next window when it becomes true.
	 *
	 * @return a {@link Flux} of {@link Flux} windows, bounded depending
	 * on the predicate.
	 */
	public Flux<Flux<T>> includeUntil(Predicate<T> boundaryTrigger) {
		return includeUntil(boundaryTrigger, false);
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows delimited by the
	 * given predicate. A new window is opened each time the predicate returns true.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window completes or errors).
	 * <p>
	 * If {@code cutBefore} is true, the old window will onComplete and the triggering
	 * element will be emitted in the new window, which becomes immediately available.
	 * This variant can emit an empty window if the sequence starts with a separator.
	 * <p>
	 * Otherwise, the triggering element will be emitted in the old window before it does
	 * onComplete, similar to {@link #includeUntil(Predicate)}. This variant shouldn't
	 * expose empty windows, as the separators are emitted into the windows they close.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilWithCutBefore.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param boundaryTrigger a predicate that triggers the next window when it becomes true.
	 * @param cutBefore set to true to include the triggering element in the new window rather than the old.
	 *
	 * @return a {@link Flux} of {@link Flux} windows, bounded depending
	 * on the predicate.
	 */
	public Flux<Flux<T>> includeUntil(Predicate<T> boundaryTrigger, boolean cutBefore) {
		return includeUntil(boundaryTrigger, cutBefore, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows delimited by the given
	 * predicate and using a prefetch. A new window is opened each time the predicate
	 * returns true.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window completes or errors).
	 * <p>
	 * If {@code cutBefore} is true, the old window will onComplete and the triggering
	 * element will be emitted in the new window. This variant can emit an empty window
	 * if the sequence starts with a separator.
	 * <p>
	 * Otherwise, the triggering element will be emitted in the old window before it does
	 * onComplete, similar to {@link #includeUntil(Predicate)}. This variant shouldn't
	 * expose empty windows, as the separators are emitted into the windows they close.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilWithCutBefore.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param boundaryTrigger a predicate that triggers the next window when it becomes true.
	 * @param cutBefore set to true to include the triggering element in the new window rather than the old.
	 * @param prefetch the request size to use for this {@link Flux}.
	 *
	 * @return a {@link Flux} of {@link Flux} windows, bounded depending
	 * on the predicate.
	 */
	public Flux<Flux<T>> includeUntil(Predicate<T> boundaryTrigger, boolean cutBefore, int prefetch) {
		return Flux.onAssembly(new FluxWindowPredicate<>(this.source,
			Queues.unbounded(prefetch),
			Queues.unbounded(prefetch),
			prefetch,
			boundaryTrigger,
			cutBefore ? FluxBufferPredicate.Mode.UNTIL_CUT_BEFORE : FluxBufferPredicate.Mode.UNTIL));
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another) into multiple {@link Flux} windows.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilChanged.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @return a {@link Flux} of {@link Flux} windows.
	 */
	public Flux<Flux<T>> includeUntilChanged() {
		return includeUntilChanged(Flux.identityFunction());
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link
	 * Function}, into multiple {@link Flux} windows.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilChangedWithKeySelector.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param keySelector function to compute comparison key for each element
	 *
	 * @return a {@link Flux} of {@link Flux} windows.
	 */
	public <V> Flux<Flux<T>> includeUntilChanged(Function<? super T, ? super V> keySelector) {
		return includeUntilChanged(keySelector, Flux.equalPredicate());
	}

	/**
	 * Collect subsequent repetitions of an element (that is, if they arrive right after
	 * one another), as compared by a key extracted through the user provided {@link
	 * Function} and compared using a supplied {@link BiPredicate}, into multiple
	 * {@link Flux} windows.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowUntilChangedWithKeySelector.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal. Upon cancellation of the current window,
	 * it also discards the remaining elements that were bound for it until the main sequence completes
	 * or creation of a new window is triggered.
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @param keyComparator predicate used to compare keys
	 *
	 * @return a {@link Flux} of {@link Flux} windows.
	 */
	public <V> Flux<Flux<T>> includeUntilChanged(Function<? super T, ? extends V> keySelector,
												 BiPredicate<? super V, ? super V> keyComparator) {
		return Flux.defer(() -> includeUntil(new FluxBufferPredicate.ChangedPredicate<T, V>
			(keySelector, keyComparator), true));
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows that stay open
	 * while a given predicate matches the source elements. Once the predicate returns
	 * false, the window closes with an onComplete and the triggering element is discarded.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window completes or errors). Empty windows
	 * can happen when a sequence starts with a separator or contains multiple separators,
	 * but a sequence that finishes with a separator won't cause a remainder empty window
	 * to be emitted.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWhile.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal, as well as the triggering element(s) (that doesn't match
	 * the predicate). Upon cancellation of the current window, it also discards the remaining elements
	 * that were bound for it until the main sequence completes or creation of a new window is triggered.
	 *
	 * @param inclusionPredicate a predicate that triggers the next window when it becomes false.
	 *
	 * @return a {@link Flux} of {@link Flux} windows, each containing
	 * subsequent elements that all passed a predicate.
	 */
	public Flux<Flux<T>> includeWhile(Predicate<T> inclusionPredicate) {
		return includeWhile(inclusionPredicate, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Split this {@link Flux} sequence into multiple {@link Flux} windows that stay open
	 * while a given predicate matches the source elements. Once the predicate returns
	 * false, the window closes with an onComplete and the triggering element is discarded.
	 * <p>
	 * Windows are lazily made available downstream at the point where they receive their
	 * first event (an element is pushed, the window completes or errors). Empty windows
	 * can happen when a sequence starts with a separator or contains multiple separators,
	 * but a sequence that finishes with a separator won't cause a remainder empty window
	 * to be emitted.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWhile.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal, as well as the triggering element(s) (that doesn't match
	 * the predicate). Upon cancellation of the current window, it also discards the remaining elements
	 * that were bound for it until the main sequence completes or creation of a new window is triggered.
	 *
	 * @param inclusionPredicate a predicate that triggers the next window when it becomes false.
	 * @param prefetch the request size to use for this {@link Flux}.
	 *
	 * @return a {@link Flux} of {@link Flux} windows, each containing
	 * subsequent elements that all passed a predicate.
	 */
	public Flux<Flux<T>> includeWhile(Predicate<T> inclusionPredicate, int prefetch) {
		return Flux.onAssembly(new FluxWindowPredicate<>(this.source,
			Queues.unbounded(prefetch),
			Queues.unbounded(prefetch),
			prefetch,
			inclusionPredicate,
			FluxBufferPredicate.Mode.WHILE));
	}

	/**
	 * Split this {@link Flux} sequence into continuous, non-overlapping windows
	 * where the window boundary is signalled by another {@link Publisher}
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWithBoundary.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * @param boundary a {@link Publisher} to emit any item for a split signal and complete to terminate
	 *
	 * @return a {@link Flux} of {@link Flux} windows delimited by a given {@link Publisher}
	 */
	public Flux<Flux<T>> splitWhen(Publisher<?> boundary) {
		return Flux.onAssembly(new FluxWindowBoundary<>(this.source,
			boundary, Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}

	/**
	 * Split this {@link Flux} sequence into potentially overlapping windows controlled by items of a
	 * start {@link Publisher} and end {@link Publisher} derived from the start values.
	 *
	 * <p>
	 * When Open signal is strictly not overlapping Close signal : dropping windows
	 * <p>
	 * When Open signal is strictly more frequent than Close signal : overlapping windows
	 * <p>
	 * When Open signal is exactly coordinated with Close signal : exact windows
	 * <p>
	 * <img class="marble" src="doc-files/marbles/windowWhen.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator DOES NOT discard elements.
	 *
	 * @param bucketOpening a {@link Publisher} that opens a new window when it emits any item
	 * @param closeSelector a {@link Function} given an opening signal and returning a {@link Publisher} that
	 * will close the window when emitting
	 * @param <U> the type of the sequence opening windows
	 * @param <V> the type of the sequence closing windows opened by the bucketOpening Publisher's elements
	 *
	 * @return a {@link Flux} of {@link Flux} windows opened by signals from a first
	 * {@link Publisher} and lasting until a selected second {@link Publisher} emits
	 */
	public <U, V> Flux<Flux<T>> when(Publisher<U> bucketOpening,
									 final Function<? super U, ? extends Publisher<V>> closeSelector) {
		return Flux.onAssembly(new FluxWindowWhen<>(this.source,
			bucketOpening,
			closeSelector,
			Queues.unbounded(Queues.XS_BUFFER_SIZE)));
	}
}
