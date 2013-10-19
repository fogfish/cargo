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
   start_link/1,
   init/1,
   client_api/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

%%
-define(QUEUE(X),  [
   {type,      reusable}
  ,{worker,    {cargo_cask_tx, [Cask]}}
  ,{capacity,  X#cask.capacity}
  ,{linger,    X#cask.linger}
]).

%%
start_link(Cask) ->
   supervisor:start_link(?MODULE, [Cask]).
   
init([Cask]) ->   
   {ok,
      {
         {one_for_one, 2, 1800},
         [
            % tx i/o pool
            ?CHILD(supervisor, pq, [Cask#cask.id, ?QUEUE(Cask)])
         ]
      }
   }.

%%
%% return pid of client api
client_api(Sup) ->
   {_, Pid, _, _} = lists:keyfind(pq, 1, supervisor:which_children(Sup)),
   pq:queue(Pid).



