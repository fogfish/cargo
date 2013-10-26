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

-include("cargo.hrl").

-export([start/0, start/1]).
-export([
	start_link/1,
	start_link/2,
	cask/1,
	cask/2,
	% cask interface

	% peer interface
	join/2,
	leave/1,
	peers/0,

	do/2,
   create/2,
   % create/3,

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
%% start storage cask (transaction pool), returns supervisor tree
%%
%%  Options:
%%     {peer,        atom()} - peer name
%%     {struct,    atom()} - struct identity
%%     {keylen, integer()} - length of key   (default 1)
%%     {property,[atom()]} - list of properties
%%     {domain,    atom()} - storage domain   
%%     {bucket,    atom()} - storage bucket  (default struct)
%%     {index,     atom()} - storage index   (default 'PRIMARY')
%%     {capacity, integer()} - pool capacity
%%     {linger,   integer()} - pool linger
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
	start_link(undefined, Opts).

start_link(Name, Opts) ->
	case supervisor:start_child(cargo_cask_root_sup, [self(), Name, Opts]) of
		{ok, Pid} -> cargo_cask_sup:client_api(Pid);
		Error     -> Error
	end.

%%
%% start storage cask components, returns pid of cask process
-spec(cask/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(cask/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

cask(Opts) ->
	cask(undefined, Opts).

cask(Name, Opts) ->
   case supervisor:start_child(cargo_cask_root_sup, [Name, Opts]) of
      {ok, Pid} -> cargo_cask_sup:client_api(Pid);
      Error     -> Error
   end.


%%%------------------------------------------------------------------
%%%
%%% peer interface
%%%
%%%------------------------------------------------------------------   

%%
%% join storage peer
%%  Options:
%%    {host,  hostname()} - peer hostname or ip address
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

%%%------------------------------------------------------------------
%%%
%%% i/o interface
%%%
%%%------------------------------------------------------------------   

%%
%% execute raw / dirty operation over i/o socket, current process is blocked 
-spec(do/2 :: (#cask{}, any()) -> {ok, any()} | {error, any()}).

do(#cask{}=Cask, Req) ->
	cargo_io:do(Cask, Req).

%%
%% create entity
create(#cask{}=Cask, Entity) ->
   cargo:do(Cask, {create, Entity}).

%%
%% 
% create(Sock, Entity) ->
%    create(Sock, erlang:element(1, Entity), Entity).

% create(Sock, Cask, Entity) ->
%    do(Sock, Cask, Entity).



%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   


t() ->
   {ok,   _} = cargo:join(test, [{host, mysqld}, {reader, 80}, {writer, 80}, {pool, 10}]),
   {ok, Pid} = cargo:cask([
      {peer,     test}
     ,{struct,   test}
     ,{property, [a,b,c]}
     ,{domain,   test}
   ]),
   {ok,   _} = cargo:cask(aaa, [
      {peer,     test}
     ,{struct,   test}
     ,{property, [a,b,c,d,e,f]}
     ,{domain,   test}
   ]),

   lager:set_loglevel(lager_console_backend, debug),
   {ok,  Tx} = pq:lease(Pid),
	plib:call(Tx, fun(X) -> tx(Pid, X) end).


tx(Pid, Cask0) ->
	{ok, Result, Cask1} = cargo_io:do(Pid, {a,b,c}, Cask0), 
   %cargo:create(IO, {a,b,c}),
	io:format("--> ~p~n", [Result]),
	{ok, _, Cask2} = cargo_io:do(aaa, req1, Cask1).


