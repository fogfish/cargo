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

%%%------------------------------------------------------------------
%%%
%%% build time config
%%%
%%%------------------------------------------------------------------   

%% enable debug output
-define(CONFIG_DEBUG,     true).

%% default i/o protocol family
-define(CONFIG_IO_FAMILY, cargo_io_hs).

%% default cask index
-define(CONFIG_INDEX,     'PRIMARY').

%%%------------------------------------------------------------------
%%%
%%% macro
%%%
%%%------------------------------------------------------------------   

%%
%% logger macro
-ifndef(ERROR).
-define(ERROR(Fmt, Args), lager:error(Fmt, Args)).
-endif.

-ifndef(DEBUG).
   -ifdef(CONFIG_DEBUG).
		-define(DEBUG(Fmt, Args), lager:debug(Fmt, Args)).
	-else.
		-define(DEBUG(Fmt, Args), ok).
	-endif.
-endif.

%%%------------------------------------------------------------------
%%%
%%% records
%%%
%%%------------------------------------------------------------------   

%%
%% dirty (raw) i/o socket (used by tx object)
-record(iosock, {
	mod  = undefined :: atom(),  % i/o protocol functor 
	pool = undefined :: pid(),   % i/o socket pool
	pid  = undefined :: pid()    % i/o socket process (leased)
}).

%%
%% cask definition
-record(cask, {
	id       = undefined  :: atom(),    % cask identity
	peer     = undefined  :: atom(),    % peer assotiated with cask
	struct   = undefined  :: atom(),    % struct identity
	keylen   = 1          :: integer(), % length of key properties 
	property = undefined  :: [atom()],  % list of properties
	domain   = undefined  :: atom(),    % storage domain
	bucket   = undefined  :: atom(),    % storage bucket
	index    = undefined  :: atom(),    % storage index    
	capacity = 100        :: integer(), % storage i/o capacity
	linger   = 100        :: integer()  % storage i/o linger   
}).

