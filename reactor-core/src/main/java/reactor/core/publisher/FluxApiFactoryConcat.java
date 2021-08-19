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

import org.reactivestreams.Publisher;

import reactor.util.concurrent.Queues;

/**
 * A set of {@link Flux} factory methods around concatenation of multiple publishers.
 *
 * @author Simon Baslé
 */
public final class FluxApiFactoryConcat {

	static final FluxApiFactoryConcat INSTANCE = new FluxApiFactoryConcat();

	FluxApiFactoryConcat() {
	}

	/**
	 * Concatenate all sources provided in an {@link Iterable}, forwarding elements
	 * emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Any error interrupts the sequence immediately and is
	 * forwarded downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatVarSources.svg" alt="">
	 *
	 * @param sources The {@link Iterable} of {@link Publisher} to concatenate
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all source sequences
	 */
	public <T> Flux<T> fromIterable(Iterable<? extends Publisher<? extends T>> sources) {
		return Flux.onAssembly(new FluxConcatIterable<>(sources));
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Any error interrupts the sequence immediately and is
	 * forwarded downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences
	 */
	public <T> Flux<T> fromPublisher(Publisher<? extends Publisher<? extends T>> sources) {
		return fromPublisher(sources, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Any error interrupts the sequence immediately and is
	 * forwarded downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param prefetch the number of Publishers to prefetch from the outer {@link Publisher}
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences
	 */
	public <T> Flux<T> fromPublisher(Publisher<? extends Publisher<? extends T>> sources, int prefetch) {
		return Flux.from(sources).concatMap(Flux.identityFunction(), prefetch);
	}

	/**
	 * Concatenate all sources provided as a vararg, forwarding elements emitted by the
	 * sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Any error interrupts the sequence immediately and is
	 * forwarded downstream.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatVarSources.svg" alt="">
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all source sequences
	 */
	@SafeVarargs
	public final <T> Flux<T> allOf(Publisher<? extends T>... sources) {
		return Flux.onAssembly(new FluxConcatArray<>(false, sources));
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Errors do not interrupt the main sequence but are propagated
	 * after the rest of the sources have had a chance to be concatenated.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 *
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences, delaying errors
	 */
	public <T> Flux<T> fromPublisherDelayError(Publisher<? extends Publisher<? extends T>> sources) {
		return fromPublisherDelayError(sources, Queues.XS_BUFFER_SIZE);
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Errors do not interrupt the main sequence but are propagated
	 * after the rest of the sources have had a chance to be concatenated.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 * <p>
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param prefetch number of elements to prefetch from the source, to be turned into inner Publishers
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences until complete or error
	 */
	public <T> Flux<T> fromPublisherDelayError(Publisher<? extends Publisher<? extends T>> sources, int prefetch) {
		return Flux.from(sources).concatMapDelayError(Flux.identityFunction(), prefetch);
	}

	/**
	 * Concatenate all sources emitted as an onNext signal from a parent {@link Publisher},
	 * forwarding elements emitted by the sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes.
	 * <p>
	 * Errors do not interrupt the main sequence but are propagated after the current
	 * concat backlog if {@code delayUntilEnd} is {@literal false} or after all sources
	 * have had a chance to be concatenated if {@code delayUntilEnd} is {@literal true}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatAsyncSources.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concatenate
	 * @param delayUntilEnd delay error until all sources have been consumed instead of
	 * after the current source
	 * @param prefetch the number of Publishers to prefetch from the outer {@link Publisher}
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all inner sources sequences until complete or error
	 */
	public <T> Flux<T> fromPublisherDelayError(Publisher<? extends Publisher<? extends T>> sources,
											   boolean delayUntilEnd, int prefetch) {
		return Flux.from(sources).concatMapDelayError(Flux.identityFunction(), delayUntilEnd, prefetch);
	}

	/**
	 * Concatenate all sources provided as a vararg, forwarding elements emitted by the
	 * sources downstream.
	 * <p>
	 * Concatenation is achieved by sequentially subscribing to the first source then
	 * waiting for it to complete before subscribing to the next, and so on until the
	 * last source completes. Errors do not interrupt the main sequence but are propagated
	 * after the rest of the sources have had a chance to be concatenated.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/concatVarSources.svg" alt="">
	 * <p>
	 *
	 * <p><strong>Discard Support:</strong> This operator discards elements it internally queued for backpressure upon cancellation.
	 *
	 * @param sources The {@link Publisher} of {@link Publisher} to concat
	 * @param <T> The type of values in both source and output sequences
	 *
	 * @return a new {@link Flux} concatenating all source sequences
	 */
	@SafeVarargs
	public final <T> Flux<T> allOfDelayError(Publisher<? extends T>... sources) {
		return Flux.onAssembly(new FluxConcatArray<>(true, sources));
	}
}
