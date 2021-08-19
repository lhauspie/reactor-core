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
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.util.context.Context;

/**
 * A {@link Flux} API sub-group that exposes an extra set of side effects, either similar to the side effects exposed
 * at top level (via {@link Flux#doOn()} with more configuration parameters, or acting on signals that are more
 * rarely considered by most users. Exposed via {@link Flux#doOn() someFlux.doOn()}'s {@link FluxApiGroupDoOnCommon#advanced() advanced()} method.
 *
 * @author Simon Baslé
 */
public final class FluxApiGroupDoOnAdvanced<T> {

	private final Flux<T> source;

	FluxApiGroupDoOnAdvanced(Flux<T> source) {
		this.source = source;
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} completes with an error matching the given exception {@link Class}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorWithClassPredicateForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError the error handler for each error
	 * @param <R> type of the error to handle
	 *
	 * @return an observed  {@link Flux}
	 */
	public <R extends Throwable> Flux<T> onError(Class<R> exceptionType, Consumer<? super R> onError) {
		Objects.requireNonNull(exceptionType, "type");
		@SuppressWarnings("unchecked")
		Consumer<Throwable> handler = (Consumer<Throwable>)onError;
		return onError(exceptionType::isInstance, handler);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} completes with an error matching the given exception {@link Predicate}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnErrorWithPredicateForFlux.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the onError signal is propagated
	 * downstream.
	 *
	 * @param predicate the matcher for exceptions to handle
	 * @param onError the error handler for each error
	 *
	 * @return an observed  {@link Flux}
	 */
	public Flux<T> onError(Predicate<? super Throwable> predicate, Consumer<? super Throwable> onError) {
		Objects.requireNonNull(predicate, "predicate");
		return FluxApiGroupDoOnCommon.doOnSignal(
			this.source,
			null,
			null,
			t -> {
				if (predicate.test(t)) {
					onError.accept(t);
				}
			},
			null, null, null, null);
	}

	/**
	 * Add behavior (side-effect) triggered after the {@link Flux} terminates, either by completing downstream successfully or with an error.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doAfterTerminateForFlux.svg" alt="">
	 * <p>
	 * The relevant signal is propagated downstream, then the {@link Runnable} is executed.
	 *
	 * @param afterTerminate the callback to call after {@link Subscriber#onComplete} or {@link Subscriber#onError}
	 *
	 * @return an observed  {@link Flux}
	 */
	public Flux<T> afterTerminate(Runnable afterTerminate) {
		Objects.requireNonNull(afterTerminate, "afterTerminate");
		return FluxApiGroupDoOnCommon.doOnSignal(this.source, null, null, null, null, afterTerminate, null, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} is cancelled.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnCancelForFlux.svg" alt="">
	 * <p>
	 * The handler is executed first, then the cancel signal is propagated upstream
	 * to the source.
	 *
	 * @param onCancel the callback to call on {@link Subscription#cancel}
	 *
	 * @return an observed  {@link Flux}
	 */
	public Flux<T> onCancel(Runnable onCancel) {
		Objects.requireNonNull(onCancel, "onCancel");
		return FluxApiGroupDoOnCommon.doOnSignal(this.source, null, null, null, null, null, null, onCancel);
	}

	/**
	 * Add behavior (side-effect) triggering a {@link LongConsumer} when this {@link Flux}
	 * receives any request.
	 * <p>
	 *     Note that non fatal error raised in the callback will not be propagated and
	 *     will simply trigger {@link Operators#onOperatorError(Throwable, Context)}.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnRequestForFlux.svg" alt="">
	 * <p>
	 * The {@link LongConsumer} is executed first, then the request signal is propagated
	 * upstream to the parent.
	 *
	 * @param consumer the consumer to invoke on each request
	 *
	 * @return an observed  {@link Flux}
	 */
	public Flux<T> onRequest(LongConsumer consumer) {
		Objects.requireNonNull(consumer, "consumer");
		return FluxApiGroupDoOnCommon.doOnSignal(this.source, null, null, null, null, null, consumer, null);
	}

	/**
	 * Add behavior (side-effect) triggered when the {@link Flux} is being subscribed,
	 * that is to say when a {@link Subscription} has been produced by the {@link Publisher}
	 * and is being passed to the {@link Subscriber#onSubscribe(Subscription)}.
	 * <p>
	 * This method is <strong>not</strong> intended for capturing the subscription and calling its methods,
	 * but for side effects like monitoring. For instance, the correct way to cancel a subscription is
	 * to call {@link Disposable#dispose()} on the Disposable returned by {@link Flux#subscribe()}.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/doOnSubscribe.svg" alt="">
	 * <p>
	 * The {@link Consumer} is executed first, then the {@link Subscription} is propagated
	 * downstream to the next subscriber in the chain that is being established.
	 *
	 * @param onSubscribe the callback to call on {@link Subscriber#onSubscribe}
	 *
	 * @return an observed  {@link Flux}
	 * @see Flux#doFirst(Runnable)
	 */
	public Flux<T> onSubscribe(Consumer<? super Subscription> onSubscribe) {
		Objects.requireNonNull(onSubscribe, "onSubscribe");
		return FluxApiGroupDoOnCommon.doOnSignal(this.source, onSubscribe, null, null, null, null, null, null);
	}
}
