package com.suse.salt.netapi.calls;

import static com.suse.salt.netapi.utils.ClientUtils.parameterizedType;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.suse.salt.netapi.calls.runner.Jobs;
import com.suse.salt.netapi.client.SaltClient;
import com.suse.salt.netapi.datatypes.AuthMethod;
import com.suse.salt.netapi.datatypes.Batch;
import com.suse.salt.netapi.datatypes.Event;
import com.suse.salt.netapi.datatypes.target.SSHTarget;
import com.suse.salt.netapi.datatypes.target.Target;
import com.suse.salt.netapi.errors.GenericError;
import com.suse.salt.netapi.event.EventListener;
import com.suse.salt.netapi.event.EventStream;
import com.suse.salt.netapi.event.JobReturnEvent;
import com.suse.salt.netapi.event.RunnerReturnEvent;
import com.suse.salt.netapi.parser.JsonParser;
import com.suse.salt.netapi.results.FullReturn;
import com.suse.salt.netapi.results.Result;
import com.suse.salt.netapi.results.Return;
import com.suse.salt.netapi.results.SSHResult;
import com.suse.salt.netapi.utils.ClientUtils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class representing a function call of a salt execution module.
 *
 * @param <R> the return type of the called function
 */
public class LocalCall<R> extends AbstractCall<R> {

    private final Optional<List<?>> arg;
    private final Optional<Map<String, ?>> kwarg;
    private final Optional<?> metadata;
    private final Optional<Integer> timeout;
    private final Optional<Integer> gatherJobTimeout;
    private final Optional<List<String>> executors;
    private final Optional<Map<String, ?>> executorOptions;

    public LocalCall(String functionName, Optional<List<?>> arg,
            Optional<Map<String, ?>> kwarg, TypeToken<R> returnType,
            Optional<?> metadata, Optional<Integer> timeout,
            Optional<Integer> gatherJobTimeout, Optional<List<String>> executors,
            Optional<Map<String, ?>> executorOptions) {
        super(functionName, returnType);
        this.arg = arg;
        this.kwarg = kwarg;
        this.metadata = metadata;
        this.timeout = timeout;
        this.gatherJobTimeout = gatherJobTimeout;
        this.executors = executors;
        this.executorOptions = executorOptions;
    }

    public LocalCall(String functionName, Optional<List<?>> arg,
                     Optional<Map<String, ?>> kwarg, TypeToken<R> returnType,
                     Optional<?> metadata, Optional<Integer> timeout,
                     Optional<Integer> gatherJobTimeout) {
        this(functionName, arg, kwarg, returnType, metadata, timeout, gatherJobTimeout,
                Optional.empty(), Optional.empty());
    }

    public LocalCall(String functionName, Optional<List<?>> arg,
            Optional<Map<String, ?>> kwarg, TypeToken<R> returnType,
            Optional<Integer> timeout, Optional<Integer> gatherJobTimeout) {
        this(functionName, arg, kwarg, returnType, Optional.empty(),
                timeout, gatherJobTimeout, Optional.empty(), Optional.empty());
    }

    public LocalCall(String functionName, Optional<List<?>> arg,
            Optional<Map<String, ?>> kwarg, TypeToken<R> returnType,
            Optional<?> metadata) {
        this(functionName, arg, kwarg, returnType, metadata, Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    public LocalCall(String functionName, Optional<List<?>> arg,
            Optional<Map<String, ?>> kwarg, TypeToken<R> returnType) {
        this(functionName, arg, kwarg, returnType, Optional.empty());
    }

    public LocalCall<R> withMetadata(Object metadata) {
        return new LocalCall<>(getFunction(), arg, kwarg, getReturnType(),
                Optional.of(metadata), timeout, gatherJobTimeout, executors, executorOptions);
    }

    public LocalCall<R> withoutMetadata() {
        return new LocalCall<>(getFunction(), arg, kwarg, getReturnType(),
                Optional.empty(), timeout, gatherJobTimeout, executors, executorOptions);
    }

    public LocalCall<R> withTimeouts(Optional<Integer> timeout,
            Optional<Integer> gatherJobTimeout) {
        return new LocalCall<>(getFunction(), arg, kwarg, getReturnType(), metadata,
                timeout, gatherJobTimeout, executors, executorOptions);
    }

    public LocalCall<R> withoutTimeouts() {
        return new LocalCall<>(getFunction(), arg, kwarg, getReturnType(), metadata,
                Optional.empty(), Optional.empty(), executors, executorOptions);
    }

    public LocalCall<R> withExecutors(Optional<List<String>> executors,
                                     Optional<Map<String, ?>> executorOptions) {
        return new LocalCall<>(getFunction(), arg, kwarg, getReturnType(), metadata,
                timeout, gatherJobTimeout, executors, executorOptions);
    }

    public LocalCall<R> withoutExecutors() {
        return new LocalCall<>(getFunction(), arg, kwarg, getReturnType(), metadata,
                timeout, gatherJobTimeout, Optional.empty(), Optional.empty());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> getPayload() {
        HashMap<String, Object> payload = new HashMap<>();
        payload.put("fun", getFunction());
        arg.ifPresent(arg -> payload.put("arg", arg));
        kwarg.ifPresent(kwarg -> payload.put("kwarg", kwarg));
        metadata.ifPresent(m -> payload.put("metadata", m));
        timeout.ifPresent(timeout -> payload.put("timeout", timeout));
        gatherJobTimeout.ifPresent(gatherJobTimeout -> payload.put("gather_job_timeout",
                gatherJobTimeout));
        executors.ifPresent(exe -> payload.put("module_executors", exe));
        executorOptions.ifPresent(opts -> payload.put("executor_opts", opts));
        return payload;
    }

    /**
     * Calls a execution module function on the given target asynchronously and
     * returns information about the scheduled job that can be used to query the result.
     * Authentication is done with the token therefore you have to login prior
     * to using this function.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param auth authentication credentials to use
     * @param batch parameter for enabling and configuring batching
     * @return information about the scheduled job
     */
    public CompletionStage<Optional<LocalAsyncResult<R>>> callAsync(final SaltClient client, Target<?> target,
                                                                    AuthMethod auth, Batch batch) {
        return callAsync(client, target, auth, Optional.of(batch));
    }

    /**
     * Calls a execution module function on the given target asynchronously and
     * returns information about the scheduled job that can be used to query the result.
     * Authentication is done with the token therefore you have to login prior
     * to using this function.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param auth authentication credentials to use
     * @param batch parameter for enabling and configuring batching
     * @return information about the scheduled job
     */
    public CompletionStage<Optional<LocalAsyncResult<R>>> callAsync(final SaltClient client, Target<?> target,
                                                                    AuthMethod auth, Optional<Batch> batch) {

        Map<String, Object> customArgs = new HashMap<>();
        batch.ifPresent(v -> customArgs.putAll(v.getParams()));

        return client.call(
                this, Client.LOCAL_ASYNC, Optional.of(target), customArgs,
                new TypeToken<Return<List<LocalAsyncResult<R>>>>(){}, auth)
                .thenApply(wrapper -> {
                    LocalAsyncResult<R> result = wrapper.getResult().get(0);
                    result.setType(getReturnType());
                    if (result.getJid() == null) {
                        return Optional.empty();
                    } else {
                        return Optional.of(result);
                    }
                });
    }

    /**
     * Calls a execution module function on the given target asynchronously and
     * returns information about the scheduled job that can be used to query the result.
     * Authentication is done with the token therefore you have to login prior
     * to using this function.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param auth authentication credentials to use
     * @return information about the scheduled job
     */
    public CompletionStage<Optional<LocalAsyncResult<R>>> callAsync(final SaltClient client, Target<?> target,
                                                                    AuthMethod auth) {
        return callAsync(client, target, auth, Optional.empty());
    }

    /**
     * Calls this salt call via the async client and returns the results
     * as they come in via the event stream.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param events the event stream to use
     * @param cancel future to cancel the action
     * @param auth authentication credentials to use
     * @param batch parameter for enabling and configuring batching
     * @return a map from minion id to future of the result.
     */
    public CompletionStage<Optional<Map<String, CompletionStage<Result<R>>>>> callAsync(
            SaltClient client,
            Target<?> target,
            AuthMethod auth,
            EventStream events,
            CompletionStage<GenericError> cancel,
            Batch batch) {
        return callAsync(client, target, auth, events, cancel, Optional.of(batch));
    }

    /**
     * Calls this salt call via the async client and returns the results
     * as they come in via the event stream.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param events the event stream to use
     * @param cancel future to cancel the action
     * @param auth authentication credentials to use
     * @param batch parameter for enabling and configuring batching
     * @return a map from minion id to future of the result.
     */
    public CompletionStage<Optional<Map<String, CompletionStage<Result<R>>>>> callAsync(
            SaltClient client,
            Target<?> target,
            AuthMethod auth,
            EventStream events,
            CompletionStage<GenericError> cancel,
            Optional<Batch> batch) {
        return callAsync(
                localCall -> localCall.callAsync(client, target, auth, batch),
                runnerCall -> runnerCall.callAsync(client, auth),
                events,
                cancel
        );
    }

    /**
     * Calls this salt call via the async client and returns the results
     * as they come in via the event stream.
     *
     * @param localAsync function providing callAsync for LocalCalls
     * @param runnerAsync function providing callAsync for RunnerCalls
     * @param events the event stream to use
     * @param cancel future to cancel the action
     * @return a map from minion id to future of the result.
     */
    public CompletionStage<Optional<Map<String, CompletionStage<Result<R>>>>> callAsync(
            Function<LocalCall<R>, CompletionStage<Optional<LocalAsyncResult<R>>>> localAsync,
            Function<RunnerCall<Map<String, R>>,
                                CompletionStage<RunnerAsyncResult<Map<String, R>>>> runnerAsync,
            EventStream events,
            CompletionStage<GenericError> cancel) {

        return localAsync.apply(this).thenApply(optLar -> {
            TypeToken<R> returnTypeToken = this.getReturnType();
            Type result = ClientUtils.parameterizedType(null,
                    Result.class, returnTypeToken.getType());
            @SuppressWarnings("unchecked")
            TypeToken<Result<R>> typeToken = (TypeToken<Result<R>>) TypeToken.get(result);

            return optLar.map(lar -> {
                Map<String, CompletableFuture<Result<R>>> futures =
                        lar.getMinions().stream().collect(Collectors.toMap(
                                mid -> mid,
                                mid -> new CompletableFuture<>()
                                )
                        );

                EventListener listener = new EventListener() {
                    @Override
                    public void notify(Event event) {
                        Optional<JobReturnEvent> jobReturnEvent = JobReturnEvent.parse(event);
                        if (jobReturnEvent.isPresent()) {
                            jobReturnEvent.ifPresent(e ->
                                    onJobReturn(lar.getJid(), e, typeToken, futures)
                            );
                        } else {
                            RunnerReturnEvent.parse(event).ifPresent(e ->
                                    onRunnerReturn(lar.getJid(), e, typeToken, futures)
                            );
                        }
                    }

                    @Override
                    public void eventStreamClosed(int code, String phrase) {
                        Result<R> error = Result.error(
                                new GenericError(
                                        "EventStream closed with reason "
                                                + phrase));
                        futures.values().forEach(f -> f.complete(error));
                    }
                };

                CompletableFuture<Void> allResolves = CompletableFuture.allOf(
                        futures.entrySet().stream().map(entry ->
                                // mask errors since CompletableFuture.allOf resolves on first error
                                entry.getValue().<Integer>handle((v, e) -> 0)
                        ).toArray(CompletableFuture[]::new)
                );

                allResolves.whenComplete((v, e) ->
                        events.removeEventListener(listener)
                );

                cancel.whenComplete((v, e) -> {
                    if (v != null) {
                        Result<R> error = Result.error(v);
                        futures.values().forEach(f -> f.complete(error));
                    } else if (e != null) {
                        futures.values().forEach(f -> f.completeExceptionally(e));
                    }
                });

                events.addEventListener(listener);

                // fire off lookup to get a result event for minions that already finished
                // before we installed the listeners
                runnerAsync.apply(Jobs.lookupJid(lar));

                return futures.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> (CompletionStage<Result<R>>) e.getValue()
                ));
            });
        });
    }

    /**
     * Calls a execution module function on the given target and synchronously
     * waits for the result. Authentication is done with the token therefore you
     * have to login prior to using this function.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param auth authentication credentials to use
     * @return a map containing the results with the minion name as key
     */
    public CompletionStage<Map<String, Result<R>>> callSync(final SaltClient client, Target<?> target,
            AuthMethod auth) {
        return callSyncHelperNonBlock(client, target, auth, Optional.empty())
                .thenApply(r -> r.get(0));
    }

    /**
     * Calls a execution module function on the given target with batching and
     * synchronously waits for the result. Authentication is done with the token
     * therefore you have to login prior to using this function.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param batch  the batch specification
     * @param auth authentication credentials to use
     * @return A list of maps with each list representing each batch, and maps containing
     * the results with the minion names as keys.
     */
    public CompletionStage<List<Map<String, Result<R>>>> callSync(final SaltClient client, Target<?> target,
            AuthMethod auth, Batch batch) {
        return callSync(client, target, auth, Optional.of(batch));
    }

    /**
     * Calls a execution module function on the given target with batching and
     * synchronously waits for the result. Authentication is done with the token
     * therefore you have to login prior to using this function.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param auth authentication credentials to use
     * @param batch parameter for enabling and configuring batching
     * @return A list of maps with each list representing each batch, and maps containing
     * the results with the minion names as keys.
     */
    public CompletionStage<List<Map<String, Result<R>>>> callSync(final SaltClient client, Target<?> target,
            AuthMethod auth, Optional<Batch> batch) {
        return callSyncHelperNonBlock(client, target, auth, batch);
    }

    // This is a big hack and should be fixed in salt: https://github.com/saltstack/salt/issues/52762
    // Salt "randomly" inject a retcode into the called function result if called in local batch mode.
    // This retcode it totally useless and unreliable since:
    // - its gets only injected if the result is an object so functions returning for example a string wont have it.
    // - it does not override it if a function already returns an object with retcode i.e cmd.runAll. Making it
    //   unreliable in its meaning.
    // - its injected directly into the functions result object which mixes function result structure with metadata
    //   specific to how a function is dispatched.
    private List<Map<String, Result<R>>> handleRetcodeBatchingHack(List<Map<String, Result<R>>> list, Type xor) {
        return list.stream().map(m -> {
            return m.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
                return e.getValue().<Result<R>>fold(err -> {
                    return err.<Result<R>>fold(
                            Result::error,
                            Result::error,
                            parsingError -> {
                                if (parsingError.getJson().isJsonObject()) {
                                    JsonObject jsonObject = parsingError.getJson().getAsJsonObject();
                                    if (jsonObject.has("retcode")) {
                                        jsonObject.remove("retcode");
                                        return JsonParser.GSON.fromJson(jsonObject, xor);
                                    } else {
                                        return Result.error(parsingError);
                                    }
                                } else {
                                    return Result.error(parsingError);
                                }
                            },
                            Result::error,
                            Result::error
                    );
                }, Result::success);
            }));
        }).collect(Collectors.toList());
    }

    /**
     * Helper to call an execution module function on the given target for batched or
     * unbatched while also providing an option to use the given credentials or to use a
     * prior created token. Synchronously waits for the result.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param batch the batch parameter, empty for unbatched
     * @param auth authentication credentials to use
     * @return A list of maps with each list representing each batch, and maps containing
     * the results with the minion names as keys. The first list is the entire
     * output for unbatched input.
     */
    private CompletionStage<List<Map<String, Result<R>>>> callSyncHelperNonBlock(
            final SaltClient client, Target<?> target, AuthMethod auth, Optional<Batch> batch) {
        Map<String, Object> customArgs = new HashMap<>();
        batch.ifPresent(v -> customArgs.putAll(v.getParams()));

        Client clientType = batch.isPresent() ? Client.LOCAL_BATCH : Client.LOCAL;

        Type xor = parameterizedType(null, Result.class, getReturnType().getType());
        Type fullret = parameterizedType(null, FullReturn.class, xor);
        Type map = parameterizedType(null, Map.class, String.class, fullret);
        Type listType = parameterizedType(null, List.class, map);
        Type wrapperType = parameterizedType(null, Return.class, listType);
        TypeToken<Return<List<Map<String, Result<R>>>>> typeToken =
                (TypeToken<Return<List<Map<String, Result<R>>>>>) TypeToken.get(wrapperType);

        if (batch.isPresent()) {
            return client.call(this,
                    clientType,
                    Optional.of(target),
                    customArgs,
                    typeToken,
                    auth)
                    .thenApply(Return::getResult)
                    .thenApply(results -> handleRetcodeBatchingHack(results, xor));
        } else {
            return client.call(this,
                    clientType,
                    Optional.of(target),
                    customArgs,
                    typeToken,
                    auth)
                    .thenApply(Return::getResult);
        }
    }

    /**
     * Call an execution module function on the given target via salt-ssh and synchronously
     * wait for the result.
     *
     * @param client SaltClient instance
     * @param target the target for the function
     * @param cfg Salt SSH configuration object
     * @param auth authentication credentials to use
     * @return a map containing the results with the minion name as key
     */
    public CompletionStage<Map<String, Result<SSHResult<R>>>> callSyncSSH(final SaltClient client,
            SSHTarget<?> target, SaltSSHConfig cfg, AuthMethod auth) {
        Map<String, Object> args = new HashMap<>();
        args.putAll(getPayload());
        args.putAll(target.getProps());

        SaltSSHUtils.mapConfigPropsToArgs(cfg, args);

        Type xor = parameterizedType(null, Result.class,
                parameterizedType(null, SSHResult.class, getReturnType().getType()));
        Type map = parameterizedType(null, Map.class, String.class, xor);
        Type listType = parameterizedType(null, List.class, map);
        Type wrapperType = parameterizedType(null, Return.class, listType);

        return client.call(this,
                Client.SSH,
                Optional.of(target),
                args,
                (TypeToken<Return<List<Map<String, Result<SSHResult<R>>>>>>)
                        TypeToken.get(wrapperType), auth)
                .thenApply(wrapper -> wrapper.getResult().get(0));
    }

    private static <R> void onRunnerReturn(
            String jid,
            RunnerReturnEvent rre,
            TypeToken<Result<R>> tt,
            Map<String, CompletableFuture<Result<R>>> targets
    ) {
        final RunnerReturnEvent.Data data = rre.getData();
        if (data.getFun().contentEquals("runner.jobs.list_job")) {
            Jobs.Info result = data.getResult(Jobs.Info.class);
            if (result.getJid().equals(jid)) {
                targets.forEach((mid, f) -> {
                    result.getResult(mid, tt).ifPresent(f::complete);
                });
            }
        }
    }

    private static <R> void onJobReturn(
            String jid,
            JobReturnEvent jre,
            TypeToken<Result<R>> tt,
            Map<String, CompletableFuture<Result<R>>> targets
    ) {
        if (jre.getJobId().contentEquals(jid)) {
            CompletableFuture<Result<R>> f = targets.get(jre.getMinionId());
            if (f != null) {
                f.complete(jre.getData().getResult(tt));
            }
        }
    }
}
