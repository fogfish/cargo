%%
%%   Copyright (c) 2012 Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%
-module(cargo).

-export([start/0, start/1]).
-export([
	join/2,
	leave/1,
	peers/0,

	do/2,

	t/0
]).

%%
%% start application
start()    -> 
	start(filename:join(["./priv", "dev.config"])).

start(Cfg) -> 
	applib:boot(?MODULE, Cfg).

%%
%% join storage peer
-spec(join/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

join(Peer, Opts) ->
	cargo_sup:join(Peer, Opts).

%%
%% leave storage peer
-spec(leave/1 :: (atom()) -> ok).

leave(Peer) ->
	cargo_sup:leave(Peer).

%%
%% list of peers
-spec(peers/0 :: () -> [atom()]).

peers() ->
	cargo_sup:peers().



%%
%% execute dirty operation over i/o socket, current process is blocked 
-spec(do/2 :: (any(), any()) -> {ok, any()} | {error, any()}).

do(Sock, Req) ->
	cargo_io:do(Sock, Req).



t() ->
	{ok, Pid} = cargo_tx:start_link(),
	plib:call(Pid, fun tx/1).


tx(IO) ->
	R = cargo:do(IO, req),
	io:format("--> ~p~n", [R]),
	cargo:do(IO, req1).


