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
	start_link/1,
	start_link/2,
	cask/1,
	% cask interface

	% peer interface
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

%%%------------------------------------------------------------------
%%%
%%% cask interface
%%%
%%%------------------------------------------------------------------   

%%
%% start storage cask components, returns supervisor tree
%%
%%  Options:
%%     {queue,   integer()}
%%     {linger,  integer()}
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
   cargo_cask_sup:start_link(Opts).

start_link(Name, Opts) ->
   cargo_cask_sup:start_link(Name, Opts).

%%
%% start storage cask components, returns pid of cask process
-spec(cask/1 :: (list()) -> {ok, pid()} | {error, any()}).

cask(Opts)
 when is_list(Opts) ->
   case start_link(Opts) of
      {ok, Pid} -> cargo_cask_sup:client_api(Pid);
      Error     -> Error
   end;

cask(Pid)
 when is_pid(Pid) ->
   cargo_cask_sup:client_api(Pid).


%%%------------------------------------------------------------------
%%%
%%% peer interface
%%%
%%%------------------------------------------------------------------   

%%
%% join storage peer
%%  Options:
%%    {host,  hostname()} - peer host name or ip address
%%    {reader, integer()} - peer reader i/o port (read-only request)
%%    {writer, integer()} - peer writer i/o port (mixed request)
%%    {pool,   integer()} - capacity of i/o queues
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
   {ok,   _} = cargo:join(test, [{peer, mysqld}, {reader, 80}, {writer, 80}, {pool, 10}]),
   {ok, Pid} = cargo:cask([{peer, test}]),
   {ok,  Tx} = pq:lease(Pid),
	plib:call(Tx, fun tx/1).


tx(IO) ->
	R = cargo:do(IO, req),
	io:format("--> ~p~n", [R]),
	cargo:do(IO, req1).


