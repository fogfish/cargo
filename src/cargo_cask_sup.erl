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
%%   cask supervisor
-module(cargo_cask_sup).
-behaviour(supervisor).

-include("cargo.hrl").

-export([
   start_link/2,
   start_link/3,
   init/1,
   %% api
   client_api/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

%%
-define(QUEUE(X),  [
   'self-release'
  ,{type,      reusable}
  ,{worker,    {cargo_cask_tx, [Cask]}}
  ,{capacity,  X#cask.capacity}
  ,{linger,    X#cask.linger}
]).

%% @todo: if Cask#cask.id is not defined set it to PID of queue (may be Sup ID should be used, expose multiple API)

%%
%%
start_link(Name, Opts) ->
   supervisor:start_link(?MODULE, [undefined, define_cask([{id, Name}|Opts])]).

start_link(Owner, Name, Opts) ->
   supervisor:start_link(?MODULE, [Owner, define_cask([{id, Name}|Opts])]).
   
init([Owner, Cask]) ->   
   {ok,
      {
         {one_for_all, 0, 1},
         [
            % cask leader
            ?CHILD(worker, cargo_cask, [Owner, Cask])
            % cask tx pool
           ,?CHILD(supervisor, pq, [Cask#cask.id, ?QUEUE(Cask)])
         ]
      }
   }.

%%
%%
child(Sup, Id) ->
   erlang:element(2,
      lists:keyfind(Id, 1, supervisor:which_children(Sup))
   ).


%%
%% return pid of client api
client_api(Sup) ->
   {ok, child(Sup, pq)}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%% build cask meta data from options 
define_cask(Opts) ->
   define_cask(Opts, #cask{}).
define_cask([{id, X} | Opts], S) ->
   define_cask(Opts, S#cask{id=X});   
define_cask([{peer, X} | Opts], S) ->
   define_cask(Opts, S#cask{peer=X});
define_cask([{struct, X} | Opts], S) ->
   define_cask(Opts, S#cask{struct=X});
define_cask([{keylen, X} | Opts], S) ->
   define_cask(Opts, S#cask{keylen=X});
define_cask([{property, X} | Opts], S) ->
   define_cask(Opts, S#cask{property=X});
define_cask([{domain, X} | Opts], S) ->
   define_cask(Opts, S#cask{domain=X});
define_cask([{bucket, X} | Opts], S) ->
   define_cask(Opts, S#cask{bucket=X});
define_cask([{index, X} | Opts], S) ->
   define_cask(Opts, S#cask{index=X});
define_cask([{capacity, X} | Opts], S) ->
   define_cask(Opts, S#cask{capacity=X});
define_cask([{linger, X} | Opts], S) ->
   define_cask(Opts, S#cask{linger=X});
define_cask([_ | Opts], S) ->
   define_cask(Opts, S);
define_cask([], Cask) ->
   S = assert_cask(Cask),
   %% lookup unique id
   Hash = erlang:phash2([S#cask.domain, S#cask.bucket, S#cask.index, S#cask.property]),
   {ok, Uid} = cargo_identity:lookup(Hash),
   S#cask{
      uid = Uid
   }.

assert_cask(#cask{peer=undefined}) ->
   exit({bagarg, peer});
assert_cask(#cask{struct=undefined}) ->
   exit({bagarg, struct});
assert_cask(#cask{property=undefined}) ->
   exit({bagarg, property});
assert_cask(#cask{domain=undefined}) ->
   exit({bagarg, domain});
assert_cask(#cask{bucket=undefined}=S) ->
   assert_cask(S#cask{bucket=S#cask.struct});
assert_cask(#cask{index=undefined}=S) ->
   assert_cask(S#cask{index=?CONFIG_DEFAULT_INDEX});
assert_cask(Cask) ->
   Cask.


