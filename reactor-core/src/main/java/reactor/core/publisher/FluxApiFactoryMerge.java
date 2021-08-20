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

import java.util.Comparator;
import java.util.Iterator;

import org.reactivestreams.Publisher;

import reactor.util.concurrent.Queues;

/**
 * A set of {@link Flux} factory methods around merging of multiple publishers.
 * Exposed through {@link Flux#fromMerging()}.
 *
 * @author Simon Baslé
 */
public final class FluxApiFactoryMerge {

	public static final FluxApiFactoryMerge INSTANCE = new FluxApiFactoryMerge();

	private FluxApiFactoryMerge() {
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an interleaved merged sequence. Unlike {@link Flux#fromConcatenating()}, inner
	 * sources are subscribed to eagerly.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeAsyncSources.svg" alt="">
	 *
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param source a {@link Publisher} of {@link Publisher} sources to merge
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}
	 */
	public <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source) {
		return merge(source,
			Queues.SMALL_BUFFER_SIZE,
			Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an interleaved merged sequence. Unlike {@link Flux#fromConcatenating()}, inner
	 * sources are subscribed to eagerly (but at most {@code concurrency} sources are
	 * subscribed to at the same time).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeAsyncSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param source a {@link Publisher} of {@link Publisher} sources to merge
	 * @param concurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}
	 */
	public <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source, int concurrency) {
		return merge(source, concurrency, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an interleaved merged sequence. Unlike {@link Flux#fromConcatenating()}, inner
	 * sources are subscribed to eagerly (but at most {@code concurrency} sources are
	 * subscribed to at the same time).
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeAsyncSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param source a {@link Publisher} of {@link Publisher} sources to merge
	 * @param concurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param prefetch the inner source request size
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}
	 */
	public <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source, int concurrency, int prefetch) {
		return Flux.onAssembly(new FluxFlatMap<>(
			Flux.from(source),
			Flux.identityFunction(),
			false,
			concurrency,
			Queues.get(concurrency),
			prefetch,
			Queues.get(prefetch)));
	}

	/**
	 * Merge data from {@link Publisher} sequences contained in an {@link Iterable}
	 * into an interleaved merged sequence. Unlike {@link Flux#fromConcatenating()}, inner
	 * sources are subscribed to eagerly.
	 * A new {@link Iterator} will be created for each subscriber.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeFixedSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param sources the {@link Iterable} of sources to merge (will be lazily iterated on subscribe)
	 * @param <I> The source type of the data sequence
	 *
	 * @return a merged {@link Flux}
	 */
	public <I> Flux<I> merge(Iterable<? extends Publisher<? extends I>> sources) {
		return merge(Flux.fromIterable(sources));
	}

	/**
	 * Merge data from {@link Publisher} sequences contained in an array / vararg
	 * into an interleaved merged sequence. Unlike {@link Flux#fromConcatenating()},
	 * sources are subscribed to eagerly.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeFixedSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param sources the array of {@link Publisher} sources to merge
	 * @param <I> The source type of the data sequence
	 *
	 * @return a merged {@link Flux}
	 */
	@SafeVarargs
	public final <I> Flux<I> merge(Publisher<? extends I>... sources) {
		return merge(Queues.XS_BUFFER_SIZE, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences contained in an array / vararg
	 * into an interleaved merged sequence. Unlike {@link Flux#fromConcatenating()},
	 * sources are subscribed to eagerly.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeFixedSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param sources the array of {@link Publisher} sources to merge
	 * @param prefetch the inner source request size
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive {@link Flux} publisher ready to be subscribed
	 */
	@SafeVarargs
	public final <I> Flux<I> merge(int prefetch, Publisher<? extends I>... sources) {
		return merge(prefetch, false, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences contained in an array / vararg
	 * into an interleaved merged sequence. Unlike {@link Flux#fromConcatenating()},
	 * sources are subscribed to eagerly.
	 * This variant will delay any error until after the rest of the merge backlog has been processed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeFixedSources.svg" alt="">
	 * <p>
	 * Note that merge is tailored to work with asynchronous sources or finite sources. When dealing with
	 * an infinite source that doesn't already publish on a dedicated Scheduler, you must isolate that source
	 * in its own Scheduler, as merge would otherwise attempt to drain it before subscribing to
	 * another source.
	 *
	 * @param sources the array of {@link Publisher} sources to merge
	 * @param prefetch the inner source request size
	 * @param <I> The source type of the data sequence
	 *
	 * @return a fresh Reactive {@link Flux} publisher ready to be subscribed
	 */
	@SafeVarargs
	public final <I> Flux<I> mergeDelayError(int prefetch, Publisher<? extends I>... sources) {
		return merge(prefetch, true, sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by their natural order).
	 * This is not a {@link Flux#sort()}, as it doesn't consider the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparingNaturalOrder.svg" alt="">
	 *
	 * @param sources {@link Publisher} sources of {@link Comparable} to merge
	 * @param <I> a {@link Comparable} merged type that has a {@link Comparator#naturalOrder() natural order}
	 * @return a merged {@link Flux} that , subscribing early but keeping the original ordering
	 */
	@SafeVarargs
	public final <I extends Comparable<? super I>> Flux<I> mergeComparing(Publisher<? extends I>... sources) {
		return mergeComparing(Queues.SMALL_BUFFER_SIZE, Comparator.naturalOrder(), sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}). This is not a {@link Flux#sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparing.svg" alt="">
	 *
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 */
	@SafeVarargs
	public final <T> Flux<T> mergeComparing(Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		return mergeComparing(Queues.SMALL_BUFFER_SIZE, comparator, sources);
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}). This is not a {@link Flux#sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparing.svg" alt="">
	 *
	 * @param prefetch the number of elements to prefetch from each source (avoiding too
	 * many small requests to the source when picking)
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 */
	@SafeVarargs
	public final <T> Flux<T> mergeComparing(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		if (sources.length == 0) {
			return Flux.empty();
		}
		if (sources.length == 1) {
			return Flux.from(sources[0]);
		}
		return Flux.onAssembly(new FluxMergeComparing<>(prefetch, comparator, false, sources));
	}

	/**
	 * Merge data from provided {@link Publisher} sequences into an ordered merged sequence,
	 * by picking the smallest values from each source (as defined by the provided
	 * {@link Comparator}). This is not a {@link Flux#sort(Comparator)}, as it doesn't consider
	 * the whole of each sequences.
	 * <p>
	 * Instead, this operator considers only one value from each source and picks the
	 * smallest of all these values, then replenishes the slot for that picked source.
	 * <p>
	 * Note that it is delaying errors until all data is consumed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeComparing.svg" alt="">
	 *
	 * @param prefetch the number of elements to prefetch from each source (avoiding too
	 * many small requests to the source when picking)
	 * @param comparator the {@link Comparator} to use to find the smallest value
	 * @param sources {@link Publisher} sources to merge
	 * @param <T> the merged type
	 * @return a merged {@link Flux} that compares latest values from each source, using the
	 * smallest value and replenishing the source that produced it
	 */
	@SafeVarargs
	public final <T> Flux<T> mergeComparingDelayError(int prefetch, Comparator<? super T> comparator, Publisher<? extends T>... sources) {
		if (sources.length == 0) {
			return Flux.empty();
		}
		if (sources.length == 1) {
			return Flux.from(sources[0]);
		}
		return Flux.onAssembly(new FluxMergeComparing<>(prefetch, comparator, true, sources));
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an ordered merged sequence. Unlike concat, the inner publishers are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in
	 * subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialAsyncSources.svg" alt="">
	 *
	 * @param sources a {@link Publisher} of {@link Publisher} sources to merge
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <T> Flux<T> mergeSequential(Publisher<? extends Publisher<? extends T>> sources) {
		return mergeSequential(sources, false, Queues.SMALL_BUFFER_SIZE,
			Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an ordered merged sequence. Unlike concat, the inner publishers are subscribed to
	 * eagerly (but at most {@code maxConcurrency} sources at a time). Unlike merge, their
	 * emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialAsyncSources.svg" alt="">
	 *
	 * @param sources a {@link Publisher} of {@link Publisher} sources to merge
	 * @param prefetch the inner source request size
	 * @param maxConcurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <T> Flux<T> mergeSequential(Publisher<? extends Publisher<? extends T>> sources,
											  int maxConcurrency, int prefetch) {
		return mergeSequential(sources, false, maxConcurrency, prefetch);
	}

	/**
	 * Merge data from {@link Publisher} sequences emitted by the passed {@link Publisher}
	 * into an ordered merged sequence. Unlike concat, the inner publishers are subscribed to
	 * eagerly (but at most {@code maxConcurrency} sources at a time). Unlike merge, their
	 * emitted values are merged into the final sequence in subscription order.
	 * This variant will delay any error until after the rest of the mergeSequential backlog has been processed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialAsyncSources.svg" alt="">
	 *
	 * @param sources a {@link Publisher} of {@link Publisher} sources to merge
	 * @param prefetch the inner source request size
	 * @param maxConcurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param <T> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <T> Flux<T> mergeSequentialDelayError(Publisher<? extends Publisher<? extends T>> sources,
														int maxConcurrency, int prefetch) {
		return mergeSequential(sources, true, maxConcurrency, prefetch);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an array/vararg
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param sources a number of {@link Publisher} sequences to merge
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	@SafeVarargs
	public final <I> Flux<I> mergeSequential(Publisher<? extends I>... sources) {
		return mergeSequential(Queues.XS_BUFFER_SIZE, false, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an array/vararg
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param prefetch the inner source request size
	 * @param sources a number of {@link Publisher} sequences to merge
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	@SafeVarargs
	public final <I> Flux<I> mergeSequential(int prefetch, Publisher<? extends I>... sources) {
		return mergeSequential(prefetch, false, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an array/vararg
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in subscription order.
	 * This variant will delay any error until after the rest of the mergeSequential backlog
	 * has been processed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param prefetch the inner source request size
	 * @param sources a number of {@link Publisher} sequences to merge
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	@SafeVarargs
	public final <I> Flux<I> mergeSequentialDelayError(int prefetch, Publisher<? extends I>... sources) {
		return mergeSequential(prefetch, true, sources);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an {@link Iterable}
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly. Unlike merge, their emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param sources an {@link Iterable} of {@link Publisher} sequences to merge
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <I> Flux<I> mergeSequential(Iterable<? extends Publisher<? extends I>> sources) {
		return mergeSequential(sources, false, Queues.SMALL_BUFFER_SIZE,
			Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an {@link Iterable}
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly (but at most {@code maxConcurrency} sources at a time). Unlike merge, their
	 * emitted values are merged into the final sequence in subscription order.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param sources an {@link Iterable} of {@link Publisher} sequences to merge
	 * @param maxConcurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param prefetch the inner source request size
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <I> Flux<I> mergeSequential(Iterable<? extends Publisher<? extends I>> sources,
											  int maxConcurrency, int prefetch) {
		return mergeSequential(sources, false, maxConcurrency, prefetch);
	}

	/**
	 * Merge data from {@link Publisher} sequences provided in an {@link Iterable}
	 * into an ordered merged sequence. Unlike concat, sources are subscribed to
	 * eagerly (but at most {@code maxConcurrency} sources at a time). Unlike merge, their
	 * emitted values are merged into the final sequence in subscription order.
	 * This variant will delay any error until after the rest of the mergeSequential backlog
	 * has been processed.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/mergeSequentialVarSources.svg" alt="">
	 *
	 * @param sources an {@link Iterable} of {@link Publisher} sequences to merge
	 * @param maxConcurrency the request produced to the main source thus limiting concurrent merge backlog
	 * @param prefetch the inner source request size
	 * @param <I> the merged type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <I> Flux<I> mergeSequentialDelayError(Iterable<? extends Publisher<? extends I>> sources,
														int maxConcurrency, int prefetch) {
		return mergeSequential(sources, true, maxConcurrency, prefetch);
	}


	// == private static factories that all methods delegate to ==
	// Note that we use instance method + singleton for the actual operators because we need to expose these as public, but
	// don't want users to have direct access to static methods in the present class.

	@SafeVarargs
	static <I> Flux<I> merge(int prefetch, boolean delayError, Publisher<? extends I>... sources) {
		if (sources.length == 0) {
			return Flux.empty();
		}
		if (sources.length == 1) {
			return Flux.from(sources[0]);
		}
		return Flux.onAssembly(new FluxMerge<>(sources,
			delayError,
			sources.length,
			Queues.get(sources.length),
			prefetch,
			Queues.get(prefetch)));
	}

	@SafeVarargs
	static <I> Flux<I> mergeSequential(int prefetch, boolean delayError,
									   Publisher<? extends I>... sources) {
		if (sources.length == 0) {
			return Flux.empty();
		}
		if (sources.length == 1) {
			return Flux.from(sources[0]);
		}
		return Flux.onAssembly(new FluxMergeSequential<>(new FluxArray<>(sources),
			Flux.identityFunction(), sources.length, prefetch,
			delayError ? FluxConcatMap.ErrorMode.END : FluxConcatMap.ErrorMode.IMMEDIATE));
	}

	static <T> Flux<T> mergeSequential(Publisher<? extends Publisher<? extends T>> sources,
									   boolean delayError, int maxConcurrency, int prefetch) {
		return Flux.onAssembly(new FluxMergeSequential<>(Flux.from(sources),
			Flux.identityFunction(),
			maxConcurrency, prefetch, delayError ? FluxConcatMap.ErrorMode.END :
			FluxConcatMap.ErrorMode.IMMEDIATE));
	}

	static <I> Flux<I> mergeSequential(Iterable<? extends Publisher<? extends I>> sources,
									   boolean delayError, int maxConcurrency, int prefetch) {
		return Flux.onAssembly(new FluxMergeSequential<>(new FluxIterable<>(sources),
			Flux.identityFunction(), maxConcurrency, prefetch,
			delayError ? FluxConcatMap.ErrorMode.END : FluxConcatMap.ErrorMode.IMMEDIATE));
	}
}
