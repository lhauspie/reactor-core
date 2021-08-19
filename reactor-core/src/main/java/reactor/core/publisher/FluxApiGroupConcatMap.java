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
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A {@link Flux} API sub-group that exposes all the concatMap operators and variants. Exposed via {@link Flux#concatMaps()}.
 *
 * @author Simon Baslé
 */
public final class FluxApiGroupConcatMap<T> {

	private final Flux<T> source;

	FluxApiGroupConcatMap(Flux<T> source) {
		this.source = source;
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link Flux#flatMaps()} (and their {@link FluxApiGroupFlatMap#sequential(Function) flatMapSequential}
	 * variants):
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors will immediately short circuit current concat backlog.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 */
	//FIXME find a better naming ? or should we use similar naming for FluxApiGroupFlatMap#interleaved ?
	public <V> Flux<V> map(Function<? super T, ? extends Publisher<? extends V>> mapper) {
		return map(mapper, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link Flux#flatMaps()} (and their {@link FluxApiGroupFlatMap#sequential(Function) flatMapSequential}
	 * variants):
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors will immediately short circuit current concat backlog. The prefetch argument
	 * allows to give an arbitrary prefetch size to the upstream source.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param prefetch the number of values to prefetch from upstream source (set it to 0 if you don't want it to prefetch)
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 */
	public <V> Flux<V> map(Function<? super T, ? extends Publisher<? extends V>> mapper, int prefetch) {
		if (prefetch == 0) {
			return Flux.onAssembly(new FluxConcatMapNoPrefetch<>(this.source, mapper, FluxConcatMap.ErrorMode.IMMEDIATE));
		}
		return Flux.onAssembly(new FluxConcatMap<>(this.source, mapper, Queues.get(prefetch), prefetch,
			FluxConcatMap.ErrorMode.IMMEDIATE));
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link Flux#flatMaps()} (and their {@link FluxApiGroupFlatMap#sequential(Function) flatMapSequential}
	 * variants):
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors in the individual publishers will be delayed at the end of the whole concat
	 * sequence (possibly getting combined into a {@link reactor.core.Exceptions#isMultiple(Throwable) composite}
	 * if several sources fail.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 */
	public <V> Flux<V> mapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper) {
		return mapDelayError(mapper, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link Flux#flatMaps()} (and their {@link FluxApiGroupFlatMap#sequential(Function) flatMapSequential}
	 * variants):
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors in the individual publishers will be delayed at the end of the whole concat
	 * sequence (possibly getting combined into a {@link reactor.core.Exceptions#isMultiple(Throwable) composite}
	 * if several sources fail.
	 * The prefetch argument allows to give an arbitrary prefetch size to the upstream source.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param prefetch the number of values to prefetch from upstream source
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 */
	public <V> Flux<V> mapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper, int prefetch) {
		return mapDelayError(mapper, true, prefetch);
	}

	/**
	 * Transform the elements emitted by this {@link Flux} asynchronously into Publishers,
	 * then flatten these inner publishers into a single {@link Flux}, sequentially and
	 * preserving order using concatenation.
	 * <p>
	 * There are three dimensions to this operator that can be compared with
	 * {@link Flux#flatMaps()} (and their {@link FluxApiGroupFlatMap#sequential(Function) flatMapSequential}
	 * variants):
	 * <ul>
	 *     <li><b>Generation of inners and subscription</b>: this operator waits for one
	 *     inner to complete before generating the next one and subscribing to it.</li>
	 *     <li><b>Ordering of the flattened values</b>: this operator naturally preserves
	 *     the same order as the source elements, concatenating the inners from each source
	 *     element sequentially.</li>
	 *     <li><b>Interleaving</b>: this operator does not let values from different inners
	 *     interleave (concatenation).</li>
	 * </ul>
	 *
	 * <p>
	 * Errors in the individual publishers will be delayed after the current concat
	 * backlog if delayUntilEnd is false or after all sources if delayUntilEnd is true.
	 * The prefetch argument allows to give an arbitrary prefetch size to the upstream source.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMap.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param mapper the function to transform this sequence of T into concatenated sequences of V
	 * @param delayUntilEnd delay error until all sources have been consumed instead of
	 * after the current source
	 * @param prefetch the number of values to prefetch from upstream source
	 * @param <V> the produced concatenated type
	 *
	 * @return a concatenated {@link Flux}
	 */
	public <V> Flux<V> mapDelayError(Function<? super T, ? extends Publisher<? extends V>> mapper,
									 boolean delayUntilEnd, int prefetch) {
		FluxConcatMap.ErrorMode errorMode =
			delayUntilEnd ? FluxConcatMap.ErrorMode.END : FluxConcatMap.ErrorMode.BOUNDARY;
		if (prefetch == 0) {
			return Flux.onAssembly(new FluxConcatMapNoPrefetch<>(this.source, mapper, errorMode));
		}
		return Flux.onAssembly(new FluxConcatMap<>(this.source, mapper, Queues.get(prefetch), prefetch, errorMode));
	}

	/**
	 * Transform the items emitted by this {@link Flux} into {@link Iterable}, then flatten the elements from those by
	 * concatenating them into a single {@link Flux}. For each iterable, {@link Iterable#iterator()} will be called
	 * at least once and at most twice.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMapIterable.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 * <p>
	 * Note that unlike {@link FluxApiGroupFlatMap#interleaved(Function)} and {@link #map(Function)}, with Iterable there is
	 * no notion of eager vs lazy inner subscription. The content of the Iterables are all played sequentially.
	 * Thus, both this method and {@link FluxApiGroupFlatMap#iterables(Function)} are equivalent offered for discoverability
	 * through both {@link Flux#concatMaps()} and {@link Flux#flatMaps()}.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite). Note that this means each {@link Iterable}'s {@link Iterable#iterator()}
	 * method could be invoked twice.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link FluxApiGroupUnsafe#influenceUpstreamToContinueOnErrors(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the error consumer (the value consumer is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a concatenation of the values from the Iterables obtained from each element in this {@link Flux}
	 */
	public <R> Flux<R> iterables(Function<? super T, ? extends Iterable<? extends R>> mapper) {
		return iterables(mapper, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into {@link Iterable}, then flatten the emissions from those by
	 * concatenating them into a single {@link Flux}.
	 * The prefetch argument allows to give an arbitrary prefetch size to the upstream source.
	 * For each iterable, {@link Iterable#iterator()} will be called at least once and at most twice.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatMapIterable.svg" alt="">
	 * <p>
	 * This operator inspects each {@link Iterable}'s {@link Spliterator} to assess if the iteration
	 * can be guaranteed to be finite (see {@link Operators#onDiscardMultiple(Iterator, boolean, Context)}).
	 * Since the default Spliterator wraps the Iterator we can have two {@link Iterable#iterator()}
	 * calls per iterable. This second invocation is skipped on a {@link Collection} however, a type which is
	 * assumed to be always finite.
	 * <p>
	 * Note that unlike {@link FluxApiGroupFlatMap#interleaved(Function)} and {@link #map(Function)}, with Iterable there is
	 * no notion of eager vs lazy inner subscription. The content of the Iterables are all played sequentially.
	 * Thus, both this method and {@link FluxApiGroupFlatMap#iterables(Function)} are equivalent offered for discoverability
	 * through both {@link Flux#concatMaps()} and {@link Flux#flatMaps()}.
	 *
	 * <p><strong>Discard Support:</strong> Upon cancellation, this operator discards {@code T} elements it prefetched and, in
	 * some cases, attempts to discard remainder of the currently processed {@link Iterable} (if it can
	 * safely ensure the iterator is finite). Note that this means each {@link Iterable}'s {@link Iterable#iterator()}
	 * method could be invoked twice.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link FluxApiGroupUnsafe#influenceUpstreamToContinueOnErrors(BiConsumer) resuming on errors}
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the error consumer (the value consumer is not invoked, as the source element will be part of the sequence). The onNext
	 * signal is then propagated as normal.
	 *
	 * @param mapper the {@link Function} to transform input sequence into N {@link Iterable}
	 * @param prefetch the number of values to request from the source upon subscription, to be transformed to {@link Iterable}
	 * @param <R> the merged output sequence type
	 *
	 * @return a concatenation of the values from the Iterables obtained from each element in this {@link Flux}
	 */
	public <R> Flux<R> iterables(Function<? super T, ? extends Iterable<? extends R>> mapper, int prefetch) {
		return Flux.onAssembly(new FluxFlattenIterable<>(this.source, mapper, prefetch,
			Queues.get(prefetch)));
	}
}
