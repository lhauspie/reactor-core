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

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A {@link Flux} API sub-group that exposes most common side effects (signals like onNext/onComplete/onError...).
 * Exposed via {@link Flux#doOn()}.
 * <p>
 * Additionally, exposes two extra sub-groups:
 * <ul>
 *     <li>{@link #advanced()}: Less common side effects (request/cancel signals) as well as more variants for the common ones.</li>
 *     <li>{@link #combinationOf(Consumer)}: common and {@link #advanced()} side effects that can be fused together.</li>
 * </ul>
 * @author Simon Baslé
 */
public final class FluxApiGroupDoOnCommon<T> {

	private final Flux<T> source;

	FluxApiGroupDoOnCommon(Flux<T> source) {
		this.source = source;
	}

	/**
	 * Offer an extra set of side effects, either similar to the side effects exposed at
	 * this level with more configuration parameters, or acting on signals that are more
	 * rarely considered by most users.
	 *
	 * @return a new {@link FluxApiGroupDoOnAdvanced}, an api group for advanced side effects
	 */
	public FluxApiGroupDoOnAdvanced<T> advanced() {
		return new FluxApiGroupDoOnAdvanced<>(this.source);
	}

	/**
	 * Offer a way to define most signal-based side-effects (both from this class and {@link #advanced()})
	 * in a block, allowing for the operators to merge (macro-fusion) as much as possible.
	 * <p>
	 * This is done by exposing a {@link FluxApiGroupSideEffects} instance to a {@link Consumer},
	 * in which users can chain the desired fuseable side-effects.
	 * //FIXME describe merging priority, peek, etc...
	 *
	 * @param sideEffectsSpec the {@link Consumer} that uses the provided {@link FluxApiGroupSideEffects}
	 * to specify which side effects to fuse into a single operator.
	 * @return a new {@link Flux} on which all the specified side effects are applied in as few steps
	 * as possible
	 */
	public Flux<T> combinationOf(Consumer<FluxApiGroupSideEffects<T>> sideEffectsSpec) {
		FluxApiGroupSideEffects<T> sideEffects = new FluxApiGroupSideEffects<>(this.source);
		sideEffectsSpec.accept(sideEffects);
		return sideEffects.decoratedFlux();
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} emits an item.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnNextForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onNext signal is propagated
	 * downstream.
	 *
	 * <p><strong>Error Mode Support:</strong> This operator supports {@link Flux#unsafe()} resuming on errors
	 * (including when fusion is enabled). Exceptions thrown by the consumer are passed to
	 * the error consumer (the value consumer is not invoked, as the source element will
	 * be part of the sequence). The onNext signal is then propagated as normal.
	 *
	 * @param onNext the callback to call on {@link Subscriber#onNext}
	 *
	 * @return an observed  {@link Flux}
	 */
	public Flux<T> next(Consumer<? super T> onNext) {
		Objects.requireNonNull(onNext, "onNext");
		return doOnSignal(this.source, null, onNext, null, null, null, null, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} completes successfully.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnComplete.svg" alt="">
	 * <p>
	 * The {@link Runnable} is executed first, then the onComplete signal is propagated
	 * downstream.
	 *
	 * @param onComplete the callback to call on {@link Subscriber#onComplete}
	 *
	 * @return an observed  {@link Flux}
	 */
	public Flux<T> complete(Runnable onComplete) {
		Objects.requireNonNull(onComplete, "onComplete");
		return doOnSignal(this.source, null, null, null, onComplete, null, null, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} completes with an error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param onError the callback to call on {@link Subscriber#onError}
	 *
	 * @return an observed  {@link Flux}
	 */
	public Flux<T> error(Consumer<? super Throwable> onError) {
		Objects.requireNonNull(onError, "onError");
		return doOnSignal(this.source, null, null, onError, null, null, null, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} terminates, either by
	 * completing successfully or failing with an error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnTerminateForFlux.svg" alt="">
	 * <p>
	 * The {@link Runnable} is executed first, then the onComplete/onError signal is propagated
	 * downstream.
	 *
	 * @param onTerminate the callback to call on {@link Subscriber#onComplete} or {@link Subscriber#onError}
	 *
	 * @return an observed  {@link Flux}
	 */
	public Flux<T> terminate(Runnable onTerminate) {
		Objects.requireNonNull(onTerminate, "onTerminate");
		return doOnSignal(this.source,
			null,
			null,
			e -> onTerminate.run(),
			onTerminate,
			null,
			null,
			null);
	}

	/**
	 * Add behavior (side-effects) triggered when the {@link Flux} emits an item, fails with an error
	 * or completes successfully. All these events are represented as a {@link Signal}
	 * that is passed to the side-effect callback. Note that this is an advanced operator,
	 * typically used for monitoring of a Flux. These {@link Signal} have a {@link Context}
	 * associated to them.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnEachForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the relevant signal is propagated
	 * downstream.
	 *
	 * @param signalConsumer the mandatory callback to call on
	 *   {@link Subscriber#onNext(Object)}, {@link Subscriber#onError(Throwable)} and
	 *   {@link Subscriber#onComplete()}
	 * @return an observed {@link Flux}
	 * @see #next(Consumer)
	 * @see #error(Consumer)
	 * @see #complete(Runnable)
	 * @see Flux#materialize()
	 * @see Signal
	 */
	public Flux<T> each(Consumer<? super Signal<T>> signalConsumer) {
		if (this.source instanceof Fuseable) {
			return Flux.onAssembly(new FluxDoOnEachFuseable<>(this.source, signalConsumer));
		}
		return Flux.onAssembly(new FluxDoOnEach<>(this.source, signalConsumer));
	}

	@SuppressWarnings("unchecked")
	static <T> Flux<T> doOnSignal(Flux<T> source,
								  @Nullable Consumer<? super Subscription> onSubscribe,
								  @Nullable Consumer<? super T> onNext,
								  @Nullable Consumer<? super Throwable> onError,
								  @Nullable Runnable onComplete,
								  @Nullable Runnable onAfterTerminate,
								  @Nullable LongConsumer onRequest,
								  @Nullable Runnable onCancel) {
		if (source instanceof Fuseable) {
			return Flux.onAssembly(new FluxPeekFuseable<>(source,
				onSubscribe,
				onNext,
				onError,
				onComplete,
				onAfterTerminate,
				onRequest,
				onCancel));
		}
		return Flux.onAssembly(new FluxPeek<>(source,
			onSubscribe,
			onNext,
			onError,
			onComplete,
			onAfterTerminate,
			onRequest,
			onCancel));
	}
}
