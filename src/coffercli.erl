%%% -*- erlang -*-
%%%
%%% This file is part of coffercli released under the Apache license 2.
%%% See the NOTICE for more information.
%%%
-module(coffercli).

-export([start/0, stop/0]).

-export([new_connection/1, new_connection/2,
         close_connection/1,
         ping/1,
         storages/1,
         storage/2,
         upload/2, upload/3, send_blob_part/2,
         bulk_upload_init/1, bulk_upload_send/2, bulk_upload_final/1,
         bulk_upload_stop/1,
         fetch_init/2, fetch/1, fetch_stop/1,
         fetch_all/2,
         delete/2,
         enumerate_init/1, enumerate/1, enumerate_stop/1,
         enumerate_all/1,
         stat/2]).

-include("coffercli.hrl").

-record(mupload, {client,
                  stream = false}).

-record(enumerate_reader, {client,
                           buffer = <<>>}).

-compile([{parse_transform, hackney_transform}]).

-type url() :: binary() | string().
-export_type([url/0]).

-type conn_opts() :: [any()].
-export_type([conn_opts/0]).

-type blobref() :: binary().
-type blobrefs() :: [blobref()].

-export_type([blobref/0, blobrefs/0]).

-type blob_info() :: {blobref(), integer()}.
-type blob_infos() :: [blob_info()].

-export_type([blob_info/0, blob_infos/0]).

-opaque connection() :: #coffer_conn{}.
-opaque storage() :: #remote_storage{}.
-opaque upload() :: #mupload{}.
-opaque fetcher() :: hackney:client().
-opaque enumerate_reader() :: #enumerate_reader{}.

-export_type([connection/0,
              storage/0,
              upload/0,
              fetcher/0,
              enumerate_reader/0]).

%% @doc Start the coffer application. Useful when testing using the shell.
start() ->
    coffercli_deps:ensure(),
    application:load(coffercli),
    coffercli_app:ensure_deps_started(),
    application:start(coffercli).

%% @doc Start the coffer application. Useful when testing using the shell.
stop() ->
    application:stop(coffercli).

%% @doc create a new connection to connect to a coffer server
-spec new_connection(URL :: url()) -> Connection :: connection().
new_connection(URL) ->
    new_connection(URL, []).

%% @doc create a new connection to connect to a coffer server
%% Args:
%% <ul>
%% <li><strong>URL</strong>: URL ro connect</li>
%%  <li><strong>Options:</strong> `[{connect_options, connect_options(),
%%  {ssl_options, ssl_options()}, Others]'</li>
%%      <li>`connect_options()': The default connect_options are
%%      `[binary, {active, false}, {packet, raw}])' . Vor valid options
%%      see the gen_tcp options.</li>
%%
%%      <li>`ssl_options()': See the ssl options from the ssl
%%      module.</li>
%%
%%      <li><em>Others options are</em>:
%%      <ul>
%%          <li>{proxy, proxy_options()}: to connect via a proxy.</li>
%%          <li>insecure: to perform "insecure" SSL connections and
%%          transfers without checking the certificate</li>
%%          <li>{connect_timeout, infinity | integer()}: timeout used when
%%          estabilishing a connection, in milliseconds. Default is 8000</li>
%%          <li>{recv_timeout, infinity | integer()}: timeout used when
%%          receiving a connection. Default is infinity</li>
%%      </ul>
%%
%%      </li>
%%
%%      <li>`proxy_options()':  options to connect by a proxy:
%%      <p><ul>
%%          <li>binary(): url to use for the proxy. Used for basic HTTP
%%          proxy</li>
%%          <li>{Host::binary(), Port::binary}: Host and port to connect,
%%          for HTTP proxy</li>
%%      </ul></p>
%%      </li>
%%  </ul>
-spec new_connection(URL :: url(), Options :: conn_opts())
    -> Connection :: connection().
new_connection(URL, Opts) when is_list(URL) ->
    new_connection(list_to_binary(URL), Opts);
new_connection(URL, Opts) ->
    Opts1 = case proplists:get_value(pool, Opts) of
        undefined ->
            PoolOpts = proplists:get_value(pool_opts, Opts, [{pool_size, 10}]),
            PoolName = list_to_atom(binary_to_list(<<"Pool:", URL/binary>>)),
            {ok, Pool} = hackney:start_pool(PoolName, PoolOpts),
            [{pool, Pool} | Opts];
        _ ->
            Opts
    end,
    #coffer_conn{url = URL,
                 options = Opts1,
                 state = active}.

%% @doc close a connection, close all the opened connections
-spec close_connection(Connection :: connection()) -> ok.
close_connection(#coffer_conn{options=Opts}) ->
    Pool = proplists:get_value(pool, Opts),
    hackney:stop_pool(Pool).


%% @doc ping a coffer instance to make sure it's alive
-spec ping(Connection :: connection()) -> pong | pang.
ping(#coffer_conn{state=closed}) ->
    {error, closed};
ping(#coffer_conn{url=URL, options=Opts}) ->
    case hackney:head(URL, [], <<>>, Opts) of
        {ok, 200, _, Client} ->
            hackney:close(Client),
            pong;
        _ ->
            pang
    end.

%% @doc get the list of storages in a coffer server instance
-spec storages(Connection :: connection()) ->
    {ok, [StorageName :: binary()]} | {error, term()}.
storages(#coffer_conn{state=closed}) ->
    {error, closed};
storages(#coffer_conn{url=URL, options=Opts}) ->
    URL1 = iolist_to_binary([URL, "/containers"]),
    case coffercli_util:request(get, URL1, [200], Opts) of
         {ok, _, _, JsonBin} ->
            JsonObj = jsx:decode(JsonBin),
            Containers = proplists:get_value(<<"containers">>, JsonObj),
            {ok, Containers};
        Error ->
            Error
    end.

%% @doc create a storage context, This context will be used in storage
%% related functions.
-spec storage(Connection :: connection(), Name :: binary() | string())
    -> Storage :: storage().
storage(#coffer_conn{url=URL, options=Opts}=Conn, Name) ->
    StorageURL = iolist_to_binary([URL, "/", Name]),
    #remote_storage{conn = Conn,
                    conn_options = Opts,
                    url = StorageURL,
                    name = Name}.

%% @doc upload a single BLOB using the standalone API
-spec upload(Storage :: storage(),
             binary() | {file, binary()}
             | {stream, blobref()}
             | {blobref(), iolist()}) -> {ok, any()} | {error, any()}.
upload(Storage, Bin) when is_binary(Bin) ->
    {ok, BlobRef} = coffercli_util:hash(Bin),
    upload(Storage, BlobRef, Bin);
upload(Storage, {file, _Name}=File) ->
    case coffercli_util:hash(File) of
        {ok, BlobRef} ->
            upload(Storage, BlobRef, File);
        Error ->
            Error
    end;
upload(Storage, {stream, BlobRef}) ->
    upload(Storage, BlobRef, stream);
upload(Storage, {BlobRef, Bin}) ->
    upload(Storage, BlobRef, Bin).

%% @doc upload a single BLOB using the standalone API
-spec upload(Storage :: storage(),
             BlobRef :: blobref(),
             iolist() | {file, binary()})-> {ok, any()} | {error, any()}.
upload(#remote_storage{url=URL, conn_options=Opts}, BlobRef, Blob) ->
    case coffercli_util:validate_ref(BlobRef) of
        ok ->
            URL1 = iolist_to_binary([URL, "/", BlobRef]),
            Headers = [{<<"Content-Type">>, <<"data/octet-stream">>},
                       {<<"Transfer-Encoding">>, <<"chunked">>}],
            case Blob of
                stream ->
                    hackney:request(put, URL1, Headers, stream, Opts);
                _ ->
                    case coffercli_util:request(put, URL1, [201], Opts,
                                                Headers, Blob) of
                        {ok, _, _, JsonBin} ->
                            JsonObj = jsx:decode(JsonBin),
                            [Received] = proplists:get_value(<<"received">>,
                                                             JsonObj),
                            {ok, parse_blob_info(Received)};
                        Error ->
                            Error
                    end
            end;
        error ->
            {error, invalid_blobref}
    end.


%% @doc when the ``stream`` is passed to the upload function, this
%% function can be used to stream BLOB chunks one by one.
send_blob_part(Client, eob) ->
    case hackney:start_response(Client) of
        {ok, 201, _Headers, Client1} ->
            {ok, JsonBin, _} = hackney:body(Client1),
            JsonObj = jsx:decode(JsonBin),
            [Received] = proplists:get_value(<<"received">>, JsonObj),
            {ok, parse_blob_info(Received)};

        {ok, Status, Headers, Client1} ->
            {ok, RespBody, _} = hackney:body(Client1),
            coffercli_util:handle_error(Status, Headers, RespBody);
        Error ->
            Error
    end;
send_blob_part(Client, Data) ->
    case hackney:stream_request_body(Data, Client) of
        {ok, Client1} ->
            {ok, Client1};
        Error ->
            Error
    end.

%% @doc initialize a multipart request to send multiple blobs at once
-spec bulk_upload_init(Storage :: storage())
    -> UploadContext :: upload().
bulk_upload_init(#remote_storage{url=URL, conn_options=Opts}) ->
    case hackney:request(post, URL, [], stream_multipart, Opts) of
        {ok, Client} ->
            {ok, #mupload{client=Client}};
        Error ->
            Error
    end.


%% @doc send blobs using the multipart api. Blobs are streamed to the
%% coffer instance
%% See the bulk upload example for more info.
-spec bulk_upload_send(UploadContext :: upload(),
                       binary() | iolist()
                       | {file, binary()}
                       | {stream, blobref()}
                       | eob)
    -> {ok, NewUploadContext :: upload()}
    | {error, term()}.
bulk_upload_send(#mupload{client=Client, stream=false}=Upload, Bin)
        when is_binary(Bin) ->
    {ok, BlobRef} = coffercli_util:hash(Bin),
    case hackney:stream_multipart_request({BlobRef, Bin}, Client) of
        {ok, Client1} ->
            {ok, Upload#mupload{client=Client1}};
        Error ->
            Error
    end;
bulk_upload_send(#mupload{client=Client, stream=true}=Upload, Bin)
        when is_binary(Bin) ->

    case hackney:stream_multipart_request({data, Bin}, Client) of
        {ok, Client1} ->
            {ok, Upload#mupload{client=Client1}};
        Error ->
            Error
    end;
bulk_upload_send(#mupload{client=Client, stream=true}=Upload, eob) ->
    case hackney:stream_multipart_request({data, eof}, Client) of
        {ok, Client1} ->
            {ok, Upload#mupload{client=Client1, stream=false}};
        Error ->
            Error
    end;
bulk_upload_send(#mupload{client=Client, stream=false}=Upload,
                 {stream, BlobRef}) ->
    case hackney:stream_multipart_request({data,
                                           {start, BlobRef, BlobRef,
                                            <<"application/octet-stream">>}},
                                         Client) of
        {ok, Client1} ->
            {ok, Upload#mupload{client=Client1, stream=true}};
        Error ->
            Error
    end;
bulk_upload_send(#mupload{client=Client, stream=false}=Upload,
                 {file, _Name}=File) ->

    case coffercli_util:hash(File) of
        {ok, BlobRef} ->
             case hackney:stream_multipart_request({BlobRef, File},
                                                   Client) of
                {ok, Client1} ->
                    {ok, Upload#mupload{client=Client1}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
bulk_upload_send(#mupload{stream=true}, _) ->
    {error, open_stream};
bulk_upload_send(_, _) ->
    {error, badarg}.

%% @doc finalize the multipart upload and return results
-spec bulk_upload_final(NewUploadContext :: upload())
    -> {ok, any()} | {error, term()}.
bulk_upload_final(#mupload{stream=true}) ->
    {error, open_stream};
bulk_upload_final(#mupload{client=Client}) ->
    case hackney:start_response(Client) of
        {ok, 201, _Headers, Client1} ->
            {ok, JsonBin, _} = hackney:body(Client1),
            JsonObj = jsx:decode(JsonBin),
            Received = proplists:get_value(<<"received">>, JsonObj, []),
            Errors = proplists:get_value(<<"errors">>, JsonObj, []),
            {ok, {[parse_blob_info(R) || R <- Received], Errors}};
        {ok, Status, Headers, Client1} ->
            {ok, RespBody, _} = hackney:body(Client1),
            coffercli_util:handle_error(Status, Headers, RespBody);
        Error ->
            Error
    end.

%% @doc stop to upload a blob
-spec bulk_upload_stop(NewUploadContext :: upload()) -> ok.
bulk_upload_stop(#mupload{client=Client}) ->
    hackney:close(Client),
    ok.

%% @doc initialize a stream to return a blob
-spec fetch_init(Storage :: storage(), Blobref :: blobref())
    -> Fetcher :: fetcher() | {error, term()}.
fetch_init(#remote_storage{url=URL, conn_options=Opts}, BlobRef) ->
    URL1 = iolist_to_binary([URL, "/", BlobRef]),
    case hackney:request(get, URL1, [], <<>>, Opts) of
        {ok, 200, _Headers, Client} ->
            {ok, Client};
        {ok, Status, Headers, Client1} ->
            {ok, RespBody, _} = hackney:body(Client1),
            coffercli_util:handle_error(Status, Headers, RespBody);
        Error ->
            Error
    end.

%% @doc fetch a BLOB chunk
-spec fetch(Fetcher :: fetcher()) ->
    {ok, Data :: binary(), NewFetcher :: fetcher()}
    | eob | {error, term()}.
fetch(Fetcher) ->
    case hackney:stream_body(Fetcher) of
        {ok, Data, Fetcher1} ->
            {ok, Data, Fetcher1};
        {done, Fetcher1} ->
            % release the socket
            hackney:close(Fetcher1),
            eob;
        Error ->
            Error
    end.

%% @ doc stop fetching a blob
-spec fetch_stop(Fetcher :: fetcher()) -> ok.
fetch_stop(Fetcher) ->
    hackney:close(Fetcher),
    ok.

%% @doc fetch all chunks from a Blob in memory
-spec fetch_all(Storage :: storage(), BlobRef :: blobref()) ->
    {ok, Data :: binary()} | {error, term()}.
fetch_all(#remote_storage{url=URL, conn_options=Opts}, BlobRef) ->
    URL1 = iolist_to_binary([URL, "/", BlobRef]),
    case hackney:request(get, URL1, [], <<>>, Opts) of
        {ok, 200, _Headers, Client} ->
            case hackney:body(Client) of
                {ok, Data, Client1} ->
                    % release the socket
                    hackney:close(Client1),
                    {ok, Data};
                Error ->
                    Error
            end;
        {ok, Status, Headers, Client1} ->
            {ok, RespBody, _} = hackney:body(Client1),
            coffercli_util:handle_error(Status, Headers, RespBody);
        Error ->
            Error
    end.

%% @doc delete a blob
-spec delete(Storage :: storage(), BlobRef :: blobref()) ->
    ok | {error, term()}.
delete(#remote_storage{url=URL, conn_options=Opts}, BlobRef) ->
    URL1 = iolist_to_binary([URL, "/", BlobRef]),
    case coffercli_util:request(delete, URL1, [202], Opts) of
        {ok, _, _, _} ->
            ok;
        Error ->
            Error
    end.


%% start a stream to enumerate blobs on a coffer instance
-spec enumerate_init(Storage :: storage())
    -> {ok, enumerate_reader()} | {error, term()}.
enumerate_init(#remote_storage{url=URL, conn_options=Opts}) ->
    case hackney:request(get, URL, [], <<>>, Opts) of
        {ok, 200, _, Client} ->
            {ok, #enumerate_reader{client=Client}};
        Error ->
            Error
    end.

%% enumerate a blob info
-spec enumerate(Reader :: enumerate_reader())
    -> {ok, BlobInfo :: blob_info(), NewReader :: enumerate_reader()}
    | done | {error, term()}.
enumerate(#enumerate_reader{buffer = <<>>}=Reader) ->
    enumerate_more(Reader);
enumerate(#enumerate_reader{buffer=Buffer}=Reader) ->
    case parse_enumerate_result(Buffer) of
        {ok,  BlobInfo, Rest} ->
            {ok, BlobInfo, Reader#enumerate_reader{buffer=Rest}};
        more ->
            enumerate_more(Reader)
    end.

%% @doc stop to enumerate
-spec enumerate_stop(Reader :: enumerate_reader()) -> ok.
enumerate_stop(#enumerate_reader{client=Client}) ->
    hackney:close(Client),
    ok.

%% @doc get list of all blobs
-spec enumerate_all(Storage :: storage()) -> blob_infos() | {error, term()}.
enumerate_all(#remote_storage{url=URL, conn_options=Opts}) ->
    case coffercli_util:request(get, URL, [200], Opts) of
        {ok, _, _, JsonBin} ->
            JsonObj = jsx:decode(JsonBin),
            Blobs = proplists:get_value(<<"blobs">>, JsonObj, []),
            [parse_blob_info(Info) || Info <- Blobs];
        Error ->
            Error
    end.


%% @doc test if some blobs has been uploaded or partially uploaded.
-spec stat(Storage :: storage(), Blobrefs :: blobrefs()) ->
    {ok, Found :: blob_infos(), HavePartially :: blob_infos()}
    | {error, term()}.
stat(#remote_storage{url=URL, conn_options=Opts}, BlobRefs) ->
    URL1 = iolist_to_binary([URL, "/stat"]),
    {_, Form} = lists:foldr(fun(BlobRef, {Inc, Acc}) ->
                    Key = iolist_to_binary([<<"blob">>,
                                            integer_to_list(Inc)]),
                    {Inc+1, [{Key, BlobRef} | Acc]}
            end, {0, []}, BlobRefs),
    case coffercli_util:request(post, URL1, [200], Opts, [],
                                {form, Form}) of
        {ok, _, _, JsonBin} ->
            JsonObj = jsx:decode(JsonBin),
            Stat = proplists:get_value(<<"stat">>, JsonObj, []),
            Partials = proplists:get_value(<<"alreadyHavePartially">>,
                                           JsonObj, []),
            {ok, Stat, Partials};
        Error ->
            Error
    end.


%% internals

parse_blob_info(Received) ->
    BlobRef = proplists:get_value(<<"blobref">>, Received),
    Size = proplists:get_value(<<"size">>, Received),
    {BlobRef, Size}.

parse_enumerate_result(<<>>) ->
    more;
parse_enumerate_result(Data) ->
    case binary:split(Data, <<"\n">>) of
        [<<"{\"blobs\": [" >>, Rest] ->
            parse_enumerate_result(Rest);
        [<<>>, Rest] ->
            parse_enumerate_result(Rest);
        [Line, Rest] ->
            Len = byte_size(Line) - 1,
            << JsonBin:Len/binary, _/binary >> = Line,
            {ok, parse_blob_info(jsx:decode(JsonBin)), Rest};
        [<<>>] ->
            more;
        [Bin] when is_binary(Bin) ->
            more;
        _ ->
            {error, invalid_line}
    end.

enumerate_more(#enumerate_reader{client=Client, buffer=Buffer}=Reader) ->
    case hackney:stream_body(Client) of
        {ok, Data, Client1} ->
            NewBuffer = << Buffer/binary, Data/binary >>,
            enumerate(Reader#enumerate_reader{client=Client1,
                                              buffer=NewBuffer});
        {done, Client1} ->
            hackney:close(Client1),
            done;
        Error ->
            Error
    end.
