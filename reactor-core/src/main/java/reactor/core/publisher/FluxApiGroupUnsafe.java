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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import reactor.util.context.Context;

/**
 * This {@link Flux} API sub-group exposes a set of specialist operators that are for advanced developers only.
 * These operators generally break some Reactive Streams rule and/or assumptions common in the majority of
 * vanilla reactor operators. As such, they are considered {@link Flux#unsafe()} for most developers.
 *
 * @author Simon Baslé
 */
public final class FluxApiGroupUnsafe<T> {

	private final Flux<T> source;

	FluxApiGroupUnsafe(Flux<T> source) {
		this.source = source;
	}

	/**
	 * Potentially modify the behavior of the <i>whole chain</i> of operators upstream of this one to
	 * conditionally clean up elements that get <i>discarded</i> by these operators.
	 * <p>
	 * The {@code discardHook} MUST be idempotent and safe to use on any instance of the desired
	 * type.
	 * Calls to this method are additive, and the order of invocation of the {@code discardHook}
	 * is the same as the order of declaration (calling {@code .filter(...).doOnDiscard(first).doOnDiscard(second)}
	 * will let the filter invoke {@code first} then {@code second} handlers).
	 * <p>
	 * Two main categories of discarding operators exist:
	 * <ul>
	 *     <li>filtering operators, dropping some source elements as part of their designed behavior</li>
	 *     <li>operators that prefetch a few elements and keep them around pending a request, but get cancelled/in error</li>
	 * </ul>
	 * WARNING: Not all operators support this instruction. The ones that do are identified in the javadoc by
	 * the presence of a <strong>Discard Support</strong> section.
	 *
	 * @param type the {@link Class} of elements in the upstream chain of operators that
	 * this cleanup hook should take into account.
	 * @param discardHook a {@link Consumer} of elements in the upstream chain of operators
	 * that performs the cleanup.
	 *
	 * @return a {@link Flux} that cleans up matching elements that get discarded upstream of it.
	 */
	public <R> Flux<T> influenceUpstreamToDiscardUsing(final Class<R> type, final Consumer<? super R> discardHook) {
		return source.contextWrite(Operators.discardLocalAdapter(type, discardHook));
	}

	/**
	 * Let compatible operators <strong>upstream</strong> recover from errors by dropping the
	 * incriminating element from the sequence and continuing with subsequent elements.
	 * The recovered error and associated value are notified via the provided {@link BiConsumer}.
	 * Alternatively, throwing from that biconsumer will propagate the thrown exception downstream
	 * in place of the original error, which is added as a suppressed exception to the new one.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorContinue.svg" alt="">
	 * <p>
	 * Note that this is a specialist operator that can make the behaviour of your reactive chain unclear.
	 * It operates on upstream, not downstream operators, it requires specific operator support to work,
	 * and the scope can easily propagate upstream into library code that didn't anticipate it
	 * (resulting in unintended behaviour.)
	 * <p>
	 * In most cases, you should instead handle the error inside the specific function which may cause
	 * it. Specifically, on each inner publisher you can use {@code doOnError} to log the error, and
	 * {@code onErrorResume(e -> Mono.empty())} to drop erroneous elements:
	 * <p>
	 * <pre>
	 * .flatMap(id -> repository.retrieveById(id)
	 *                          .doOnError(System.err::println)
	 *                          .onErrorResume(e -> Mono.empty()))
	 * </pre>
	 * <p>
	 * This has the advantage of being much clearer, has no ambiguity in regard to operator support,
	 * and cannot leak upstream.
	 *
	 * @param errorConsumer a {@link BiConsumer} fed with errors matching the predicate and the value
	 * that triggered the error.
	 *
	 * @return a {@link Flux} indicating to its upstream operators that they should strive to continue processing in case of errors.
	 */
	public Flux<T> influenceUpstreamToContinueOnErrors(BiConsumer<Throwable, Object> errorConsumer) {
		BiConsumer<Throwable, Object> genericConsumer = errorConsumer;
		return source.contextWrite(Context.of(
			OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
			OnNextFailureStrategy.resume(genericConsumer)
		));
	}

	/**
	 * Let compatible operators <strong>upstream</strong> recover from errors by dropping the
	 * incriminating element from the sequence and continuing with subsequent elements.
	 * Only errors matching the specified {@code type} are recovered from.
	 * The recovered error and associated value are notified via the provided {@link BiConsumer}.
	 * Alternatively, throwing from that biconsumer will propagate the thrown exception downstream
	 * in place of the original error, which is added as a suppressed exception to the new one.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorContinueWithClassPredicate.svg" alt="">
	 * <p>
	 * Note that this is a specialist operator that can make the behaviour of your reactive chain unclear.
	 * It operates on upstream, not downstream operators, it requires specific operator support to work,
	 * and the scope can easily propagate upstream into library code that didn't anticipate it
	 * (resulting in unintended behaviour.)
	 * <p>
	 * In most cases, you should instead handle the error inside the specific function which may cause
	 * it. Specifically, on each inner publisher you can use {@code doOnError} to log the error, and
	 * {@code onErrorResume(e -> Mono.empty())} to drop erroneous elements:
	 * <p>
	 * <pre>
	 * .flatMap(id -> repository.retrieveById(id)
	 *                          .doOnError(MyException.class, System.err::println)
	 *                          .onErrorResume(MyException.class, e -> Mono.empty()))
	 * </pre>
	 * <p>
	 * This has the advantage of being much clearer, has no ambiguity in regard to operator support,
	 * and cannot leak upstream.
	 *
	 * @param type the {@link Class} of {@link Exception} that are resumed from.
	 * @param errorConsumer a {@link BiConsumer} fed with errors matching the {@link Class}
	 * and the value that triggered the error.
	 *
	 * @return a {@link Flux} indicating to its upstream operators that they should strive to continue processing in case of errors.
	 */
	public <E extends Throwable> Flux<T> influenceUpstreamToContinueOnErrors(Class<E> type,
																			 BiConsumer<Throwable, Object> errorConsumer) {
		return influenceUpstreamToContinueOnErrors(type::isInstance, errorConsumer);
	}

	/**
	 * Let compatible operators <strong>upstream</strong> recover from errors by dropping the
	 * incriminating element from the sequence and continuing with subsequent elements.
	 * Only errors matching the {@link Predicate} are recovered from (note that this
	 * predicate can be applied several times and thus must be idempotent).
	 * The recovered error and associated value are notified via the provided {@link BiConsumer}.
	 * Alternatively, throwing from that biconsumer will propagate the thrown exception downstream
	 * in place of the original error, which is added as a suppressed exception to the new one.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/onErrorContinueWithPredicate.svg" alt="">
	 * <p>
	 * Note that this is a specialist operator that can make the behaviour of your reactive chain unclear.
	 * It operates on upstream, not downstream operators, it requires specific operator support to work,
	 * and the scope can easily propagate upstream into library code that didn't anticipate it
	 * (resulting in unintended behaviour.)
	 * <p>
	 * In most cases, you should instead handle the error inside the specific function which may cause
	 * it. Specifically, on each inner publisher you can use {@code doOnError} to log the error, and
	 * {@code onErrorResume(e -> Mono.empty())} to drop erroneous elements:
	 * <p>
	 * <pre>
	 * .flatMap(id -> repository.retrieveById(id)
	 *                          .doOnError(errorPredicate, System.err::println)
	 *                          .onErrorResume(errorPredicate, e -> Mono.empty()))
	 * </pre>
	 * <p>
	 * This has the advantage of being much clearer, has no ambiguity in regard to operator support,
	 * and cannot leak upstream.
	 *
	 * @param errorPredicate a {@link Predicate} used to filter which errors should be resumed from.
	 * This MUST be idempotent, as it can be used several times.
	 * @param errorConsumer a {@link BiConsumer} fed with errors matching the predicate and the value
	 * that triggered the error.
	 *
	 * @return a {@link Flux} that attempts to continue processing on some errors.
	 */
	public <E extends Throwable> Flux<T> influenceUpstreamToContinueOnErrors(Predicate<E> errorPredicate,
																			 BiConsumer<Throwable, Object> errorConsumer) {
		//this cast is ok as only T values will be propagated in this sequence
		@SuppressWarnings("unchecked")
		Predicate<Throwable> genericPredicate = (Predicate<Throwable>) errorPredicate;
		BiConsumer<Throwable, Object> genericErrorConsumer = errorConsumer;
		return source.contextWrite(Context.of(
			OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
			OnNextFailureStrategy.resumeIf(genericPredicate, genericErrorConsumer)
		));
	}

	/**
	 * If a {@link #influenceUpstreamToContinueOnErrors(BiConsumer)} variant has been used downstream, reverts
	 * to the default 'STOP' mode where errors are terminal events upstream. It can be
	 * used for easier scoping of the on next failure strategy or to override the
	 * inherited strategy in a sub-stream (for example in a flatMap). It has no effect if
	 * {@link #influenceUpstreamToContinueOnErrors(BiConsumer)} has not been used downstream.
	 *
	 * @return a {@link Flux} that terminates on errors, even if {@link #influenceUpstreamToContinueOnErrors(BiConsumer)}
	 * was used downstream
	 */
	public Flux<T> stopInfluencingUpstreamToContinueOnErrors() {
		return source.contextWrite(Context.of(
			OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
			OnNextFailureStrategy.stop()));
	}
}
