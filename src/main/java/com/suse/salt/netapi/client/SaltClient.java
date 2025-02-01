package com.suse.salt.netapi.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.suse.salt.netapi.AuthModule;
import com.suse.salt.netapi.calls.Call;
import com.suse.salt.netapi.calls.Client;
import com.suse.salt.netapi.calls.SaltSSHConfig;
import com.suse.salt.netapi.calls.SaltSSHUtils;
import com.suse.salt.netapi.datatypes.AuthMethod;
import com.suse.salt.netapi.datatypes.Token;
import com.suse.salt.netapi.datatypes.cherrypy.Stats;
import com.suse.salt.netapi.datatypes.target.Target;
import com.suse.salt.netapi.event.EventListener;
import com.suse.salt.netapi.event.WebSocketEventStream;
import com.suse.salt.netapi.exception.SaltException;
import com.suse.salt.netapi.parser.JsonParser;
import com.suse.salt.netapi.results.Result;
import com.suse.salt.netapi.results.SSHRawResult;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Salt API client.
 */
public class SaltClient {

    /** The async connection factory object */
    private final AsyncHttpClient asyncHttpClient;
    private final URI uri;

    private final Gson gson = new GsonBuilder().create();

    /**
     * Constructor for connecting to a given URL.
     *
     * @param asyncHttpClient http client to use for the salt api
     * @param url the Salt API URL
     */
    public SaltClient(URI url, AsyncHttpClient asyncHttpClient) {
        this.uri = url.resolve("/");
        this.asyncHttpClient = asyncHttpClient;
    }

    /**
     * Non-blocking version of login() returning a CompletionStage with the token.
     * <p>
     * {@code POST /login}
     *
     * @param username the username
     * @param password the password
     * @param eauth the eauth type
     * @return CompletionStage holding the authentication token
     */
    public CompletionStage<Token> login(final String username,
            final String password, final AuthModule eauth) {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("username", username);
        props.put("password", password);
        props.put("eauth", eauth.getValue());

        String payload = gson.toJson(props);

        CompletionStage<Token> result = asyncHttpClient
                .post(uri.resolve("login"), payload, JsonParser.TOKEN)
                .thenApply(r -> {
                    // They return a list of tokens here, take the first one
                    Token token = r.getResult().get(0);
                    return token;
                });

        return result;
    }

    /**
     * Perform logout and clear the session token from the config.
     * <p>
     * {@code POST /logout}
     *
     * @return true if the logout was successful, otherwise false
     */
    public CompletionStage<Boolean> logout() {
        return asyncHttpClient
                .post(uri.resolve("logout"), "", JsonParser.STRING)
                .thenApply(s -> "Your token has been cleared".contentEquals(s.getResult()));
    }

    /**
     * Generic interface to start any execution command bypassing normal session handling.
     * <p>
     * {@code POST /run}
     *
     * @param <T> type of the tgt property for this command
     * @param username the username
     * @param password the password
     * @param eauth the eauth type
     * @param client the client
     * @param target the target
     * @param function the function to execute
     * @param args list of non-keyword arguments
     * @param kwargs map containing keyword arguments
     * @return Map key: minion id, value: command result from that minion
     */
    public <T> CompletionStage<Map<String, Object>> run(final String username, final String password,
            final AuthModule eauth, final String client, final Target<T> target,
            final String function, List<Object> args, Map<String, Object> kwargs) {
        Map<String, Object> props = new HashMap<>();
        props.put("username", username);
        props.put("password", password);
        props.put("eauth", eauth.getValue());
        props.put("client", client);
        props.putAll(target.getProps());
        props.put("fun", function);
        props.put("arg", args);
        props.put("kwarg", kwargs);

        List<Map<String, Object>> list =  Collections.singletonList(props);

        String payload = gson.toJson(list);

        CompletionStage<Map<String, Object>> result = asyncHttpClient
                .post(uri.resolve("run"), payload, JsonParser.RUN_RESULTS)
                .thenApply(s -> s.getResult().get(0));
        return result;
    }

    /**
     * Calls salt-ssh with a command in raw shell mode (commands bypass Salt and
     * gets executed as shell commands).
     *
     * @param <T> type of the tgt property for this command
     * @param command to be executed
     * @param target glob type, targets to be reached by the command
     * @param cfg SaltSSH config holder
     * @return a map in which every key is a host associated to the result of the
     * raw command
     */
    public <T> CompletionStage<Map<String, Result<SSHRawResult>>> runRawSSHCommand(final String command,
            final Target<T> target, SaltSSHConfig cfg) {
        Map<String, Object> props = new HashMap<>();
        props.put("client", Client.SSH.getValue());
        props.putAll(target.getProps());
        props.put("fun", command);
        props.put("raw_shell", true);

        SaltSSHUtils.mapConfigPropsToArgs(cfg, props);

        List<Map<String, Object>> list = Collections.singletonList(props);

        String payload = gson.toJson(list);

        CompletionStage<Map<String, Result<SSHRawResult>>> result = asyncHttpClient
                .post(uri.resolve("run"), payload, JsonParser.RUNSSHRAW_RESULTS)
                .thenApply(r -> r.getResult().get(0));

        return result;
    }

    /**
     * Query statistics from the CherryPy Server.
     * <p>
     * {@code GET /stats}
     *
     * @return the stats
     */
    public CompletionStage<Stats> stats() {
        return asyncHttpClient.get(uri.resolve("stats"), JsonParser.STATS);
    }

    /**
     * Returns a WebSocket @ClientEndpoint annotated object connected
     * to the /ws ServerEndpoint.
     * <p>
     * The stream object supports the {@link WebSocketEventStream} interface which allows the caller
     * to register/unregister for stream event notifications as well as close the event
     * stream.
     * <p>
     * Note: {@link SaltClient#login(String, String, AuthModule)} or must be called prior
     * to calling this method.
     * <p>
     * {@code GET /events}
     *
     * @param listeners event listeners to be added before the stream is initialized
     * @param idleTimeout idle timeout to pass to the http client config
     * @param maxMsgSize maximum event data size to accept
     * @param sessionIdleTimeout session idle timeout to pass to the http client config
     * @param token salt session token to use for authentication
     * @return the event stream
     * @throws SaltException in case of an error during websocket stream initialization
     */
    public WebSocketEventStream events(Token token, long sessionIdleTimeout, long idleTimeout,
                       int maxMsgSize, EventListener... listeners) throws SaltException {
        return new WebSocketEventStream(uri, token, sessionIdleTimeout, idleTimeout, maxMsgSize, listeners);
    }

    public <R> CompletionStage<R> call(Call<?> call, Client client, Optional<Target<?>> target,
                Map<String, Object> custom, TypeToken<R> type, AuthMethod auth) {
        Map<String, String> headers = new HashMap<>();
        Map<String, Object> props = new HashMap<>();
        auth.getInternal().consume(token -> {
            headers.put("X-Auth-Token", token.getToken());
        }, pass -> {
                props.put("username", pass.getUsername());
                props.put("password", pass.getPassword());
                props.put("eauth", pass.getModule().getValue());
            });

        target.ifPresent(t -> props.putAll(t.getProps()));
        props.put("full_return", true);
        props.put("client", client.getValue());
        props.putAll(call.getPayload());
        props.putAll(custom);
        List<Map<String, Object>> list = Collections.singletonList(props);
        String payload = gson.toJson(list);

        URI endpoint = auth.getInternal().isRight() ? uri.resolve("run") : uri;
        return asyncHttpClient.post(endpoint, headers, payload, new JsonParser<>(type));
    }

}
