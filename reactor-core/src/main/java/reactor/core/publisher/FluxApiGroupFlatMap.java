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

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static reactor.core.publisher.Flux.identityFunction;

/**
 * A {@link Flux} API sub-group that offers all the flavors of flatMapping operators.
 * Exposed via {@link Flux#flatMapExtras()}.
 *
 * @author Simon Baslé
 */
public final class FluxApiGroupFlatMap<T> {

	private final Flux<T> source;

	FluxApiGroupFlatMap(Flux<T> source) {
		this.source = source;
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux} through merging,
	 * which allow them to interleave.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #sequential(Function) sequential flatMap} and {@link Flux#concatMapExtras()}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator does not necessarily preserve
	 *     original ordering, as inner element are flattened as they arrive.</li>
	 *     <li><b>Interleaving</b>: this operator lets values from different inners interleave
	 *     (similar to merging the inner sequences).</li>
	 * </ul>
	 * The concurrency argument allows to control how many {@link Publisher} can be
	 * subscribed to and merged in parallel. The prefetch argument allows to give an
	 * arbitrary prefetch size to the merged {@link Publisher}. This variant will delay
	 * any error until after the rest of the flatMap backlog has been processed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapWithConcurrencyAndPrefetch.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements internally queued for backpressure
	 * upon cancellation or error triggered by a data signal.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link Flux#unsafe()} resuming on errors
	 * in the mapper {@link Function}. Exceptions thrown by the mapper then behave as if
	 * it had mapped the value to an empty publisher. If the mapper does map to a scalar
	 * publisher (an optimization in which the value can be resolved immediately without
	 * subscribing to the publisher, e.g. a {@link Mono#fromCallable(Callable)}) but said
	 * publisher throws, this can be resumed from in the same manner.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param concurrency the maximum number of in-flight inner sequences
	 * @param prefetch the maximum in-flight elements from each inner {@link Publisher} sequence
	 * @param <V> the merged output sequence type
	 *
	 * @return a merged {@link Flux}
	 */
	public <V> Flux<V> delayError(Function<? super T, ? extends Publisher<? extends V>> mapper,
								  int concurrency, int prefetch) {
		return interleaved(this.source, mapper, true, concurrency, prefetch);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into {@link Iterable}, then flatten the elements from those by
	 * merging them into a single {@link Flux}. For each iterable, {@link Iterable#iterator()} will be called at least
	 * once and at most twice.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapIterableForFlux.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 * <p>
	 * Note that unlike {@link Flux#flatMap(Function)} and {@link Flux#concatMap(Function)}, with Iterable there is
	 * no notion of eager vs lazy inner subscription. The content of the Iterables are all played sequentially.
	 * Thus, both this method and {@link FluxApiGroupConcatMap#iterable(Function)} are equivalent offered for discoverability
	 * through both {@link Flux#concatMapExtras()} and {@link Flux#flatMapExtras()}.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite). Note that this means each {@link Iterable}'s {@link Iterable#iterator()}
	 * method could be invoked twice.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link Flux#unsafe()} resuming on errors
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the {@link FluxApiGroupUnsafe#influenceUpstreamToContinueOnErrors(BiConsumer)} error consumer (the value consumer
	 * is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a concatenation of the values from the Iterables obtained from each element in this {@link Flux}
	 */
	public <R> Flux<R> iterable(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return iterable(mapper, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into {@link Iterable}, then flatten the emissions from those by
	 * merging them into a single {@link Flux}. The prefetch argument allows to give an
	 * arbitrary prefetch size to the upstream source.
	 * For each iterable, {@link Iterable#iterator()} will be called at least once and at most twice.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapIterableForFlux.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 * <p>
	 * Note that unlike {@link Flux#flatMap(Function, int, int)} and {@link Flux#concatMap(Function, int)}, with Iterable there is
	 * no notion of eager vs lazy inner subscription. The content of the Iterables are all played sequentially.
	 * Thus, both this method and {@link FluxApiGroupConcatMap#iterable(Function, int)} are equivalent offered for discoverability
	 * through both {@link Flux#concatMapExtras()} and {@link Flux#flatMapExtras()}.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite).
	 * Note that this means each {@link Iterable}'s {@link Iterable#iterator()} method could be invoked twice.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link Flux#unsafe()} resuming on errors
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the {@link FluxApiGroupUnsafe#influenceUpstreamToContinueOnErrors(BiConsumer)}  error consumer (the value consumer
	 * is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N {@link Iterable}
	 * @param prefetch the number of values to request from the source upon subscription, to be transformed to {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a concatenation of the values from the Iterables obtained from each element in this {@link Flux}
	 */
	public <R> Flux<R> iterable(Function<? super T, ? extends Iterable<? extends R>> mapper,
								int prefetch) {
		return Flux.onAssembly(new FluxFlattenIterable<>(this.source, mapper, prefetch,
			Queues.get(prefetch)));
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, but merge them in
	 * the order of their source element.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link Flux#flatMap(Function)} and {@link Flux#concatMapExtras()}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners (like flatMap).</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator queues elements from
	 *     late inners until all elements from earlier inners have been emitted, thus emitting
	 *     inner sequences as a whole, in an order that matches their source's order.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (similar looking result to concatMap, but due to queueing of values
	 *     that would have been interleaved otherwise).</li>
	 * </ul>
	 *
	 * <p>
	 * That is to say, whenever a source element is emitted it is transformed to an inner
	 * {@link Publisher}. However, if such an early inner takes more time to complete than
	 * subsequent faster inners, the data from these faster inners will be queued until
	 * the earlier inner completes, so as to maintain source ordering.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapSequential.svg" alt="">
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <R> Flux<R> sequential(Function<? super T, ? extends
		Publisher<? extends R>> mapper) {
		return sequential(mapper, Queues.SMALL_BUFFER_SIZE);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, but merge them in
	 * the order of their source element.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 *  {@link Flux#flatMap(Function)} and {@link Flux#concatMapExtras()}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners (like flatMap).</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator queues elements from
	 *     late inners until all elements from earlier inners have been emitted, thus emitting
	 *     inner sequences as a whole, in an order that matches their source's order.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (similar looking result to concatMap, but due to queueing of values
	 *     that would have been interleaved otherwise).</li>
	 * </ul>
	 *
	 * <p>
	 * That is to say, whenever a source element is emitted it is transformed to an inner
	 * {@link Publisher}. However, if such an early inner takes more time to complete than
	 * subsequent faster inners, the data from these faster inners will be queued until
	 * the earlier inner completes, so as to maintain source ordering.
	 *
	 * <p>
	 * The concurrency argument allows to control how many merged {@link Publisher} can happen in parallel.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapSequentialWithConcurrency.svg" alt="">
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param maxConcurrency the maximum number of in-flight inner sequences
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <R> Flux<R> sequential(Function<? super T, ? extends
		Publisher<? extends R>> mapper, int maxConcurrency) {
		return sequential(mapper, maxConcurrency, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, but merge them in
	 * the order of their source element.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 *  {@link Flux#flatMap(Function)} and {@link Flux#concatMapExtras()}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners (like flatMap).</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator queues elements from
	 *     late inners until all elements from earlier inners have been emitted, thus emitting
	 *     inner sequences as a whole, in an order that matches their source's order.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (similar looking result to concatMap, but due to queueing of values
	 *     that would have been interleaved otherwise).</li>
	 * </ul>
	 *
	 * <p>
	 * That is to say, whenever a source element is emitted it is transformed to an inner
	 * {@link Publisher}. However, if such an early inner takes more time to complete than
	 * subsequent faster inners, the data from these faster inners will be queued until
	 * the earlier inner completes, so as to maintain source ordering.
	 *
	 * <p>
	 * The concurrency argument allows to control how many merged {@link Publisher}
	 * can happen in parallel. The prefetch argument allows to give an arbitrary prefetch
	 * size to the merged {@link Publisher}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapSequentialWithConcurrencyAndPrefetch.svg" alt="">
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param maxConcurrency the maximum number of in-flight inner sequences
	 * @param prefetch the maximum in-flight elements from each inner {@link Publisher} sequence
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <R> Flux<R> sequential(Function<? super T, ? extends
		Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
		return sequential(this.source, mapper, false, maxConcurrency, prefetch);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, but merge them in
	 * the order of their source element.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 *  {@link Flux#flatMap(Function)} and {@link Flux#concatMapExtras()}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners (like flatMap).</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator queues elements from
	 *     late inners until all elements from earlier inners have been emitted, thus emitting
	 *     inner sequences as a whole, in an order that matches their source's order.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (similar looking result to concatMap, but due to queueing of values
	 *     that would have been interleaved otherwise).</li>
	 * </ul>
	 *
	 * <p>
	 * That is to say, whenever a source element is emitted it is transformed to an inner
	 * {@link Publisher}. However, if such an early inner takes more time to complete than
	 * subsequent faster inners, the data from these faster inners will be queued until
	 * the earlier inner completes, so as to maintain source ordering.
	 *
	 * <p>
	 * The concurrency argument allows to control how many merged {@link Publisher}
	 * can happen in parallel. The prefetch argument allows to give an arbitrary prefetch
	 * size to the merged {@link Publisher}. This variant will delay any error until after the
	 * rest of the flatMap backlog has been processed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapSequentialWithConcurrencyAndPrefetch.svg" alt="">
	 *
	 * @param mapper the {@link Function} to transform input sequence into N sequences {@link Publisher}
	 * @param maxConcurrency the maximum number of in-flight inner sequences
	 * @param prefetch the maximum in-flight elements from each inner {@link Publisher} sequence
	 * @param <R> the merged output sequence type
	 *
	 * @return a merged {@link Flux}, subscribing early but keeping the original ordering
	 */
	public <R> Flux<R> sequentialDelayError(Function<? super T, ? extends
		Publisher<? extends R>> mapper, int maxConcurrency, int prefetch) {
		return sequential(this.source, mapper, true, maxConcurrency, prefetch);
	}


	/**
	 * Transform the signals emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux} through merging,
	 * which allow them to interleave. Note that at least one of the signal mappers must
	 * be provided, and all provided mappers must produce a publisher.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link #sequential(Function) sequential flatMap} and {@link Flux#concatMapExtras()}:
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator is eagerly
	 *     subscribing to its inners.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator does not necessarily preserve
	 *     original ordering, as inner element are flattened as they arrive.</li>
	 *     <li><b>Interleaving</b>: this operator lets values from different inners interleave
	 *     (similar to merging the inner sequences).</li>
	 * </ul>
	 * <p>
	 * OnError will be transformed into completion signal after its mapping callback has been applied.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/flatMapWithMappersOnTerminalEventsForFlux.svg" alt="">
	 *
	 * @param mapperOnNext the {@link Function} to call on next data and returning a sequence to merge.
	 * Use {@literal null} to ignore (provided at least one other mapper is specified).
	 * @param mapperOnError the {@link Function} to call on error signal and returning a sequence to merge.
	 * Use {@literal null} to ignore (provided at least one other mapper is specified).
	 * @param mapperOnComplete the {@link Function} to call on complete signal and returning a sequence to merge.
	 * Use {@literal null} to ignore (provided at least one other mapper is specified).
	 * @param <R> the output {@link Publisher} type target
	 *
	 * @return a new {@link Flux}
	 */
	public <R> Flux<R> signals(@Nullable Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
							   @Nullable Function<? super Throwable, ? extends Publisher<? extends R>> mapperOnError,
							   @Nullable Supplier<? extends Publisher<? extends R>> mapperOnComplete) {
		return Flux.onAssembly(new FluxFlatMap<>(
			new FluxMapSignal<>(this.source, mapperOnNext, mapperOnError, mapperOnComplete),
			identityFunction(),
			false, Queues.XS_BUFFER_SIZE,
			Queues.xs(), Queues.XS_BUFFER_SIZE,
			Queues.xs()
		));
	}

	static <T, V> Flux<V> interleaved(Flux<T> source, Function<? super T, ? extends Publisher<? extends V>> mapper,
									  boolean delayError, int concurrency, int prefetch) {
		return Flux.onAssembly(new FluxFlatMap<>(
			source,
			mapper,
			delayError,
			concurrency,
			Queues.get(concurrency),
			prefetch,
			Queues.get(prefetch)
		));
	}

	private static <T, R> Flux<R> sequential(Flux<T> source, Function<? super T, ? extends Publisher<? extends R>> mapper,
											 boolean delayError, int maxConcurrency, int prefetch) {
		return Flux.onAssembly(new FluxMergeSequential<>(source, mapper, maxConcurrency,
			prefetch, delayError ? FluxConcatMap.ErrorMode.END :
			FluxConcatMap.ErrorMode.IMMEDIATE));
	}
}
