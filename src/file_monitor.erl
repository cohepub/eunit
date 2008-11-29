%% This library is free software; you can redistribute it and/or modify
%% it under the terms of the GNU Lesser General Public License as
%% published by the Free Software Foundation; either version 2 of the
%% License, or (at your option) any later version.
%%
%% This library is distributed in the hope that it will be useful, but
%% WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
%% Lesser General Public License for more details.
%%
%% You should have received a copy of the GNU Lesser General Public
%% License along with this library; if not, write to the Free Software
%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
%% USA
%%
%% $Id$ 
%%
%% @private (for now)
%% @author Richard Carlsson <richardc@it.uu.se>
%% @copyright 2006 Richard Carlsson
%% @doc Erlang file monitoring service

-module(file_monitor).

-behaviour(gen_server).

-export([start/0, start/1, start/2, start_link/0, start_link/1,
	 start_link/2, stop/0, stop/1, init/1, monitor_file/2,
	 monitor_file/3, monitor_dir/2, monitor_dir/3, demonitor/1,
	 demonitor/2, get_poll_time/0, get_poll_time/1, set_poll_time/1,
	 set_poll_time/2]).

-export([handle_call/3, handle_cast/2, handle_info/2, code_change/3,
	 terminate/2]).

%% -export([test/0]).

-include_lib("kernel/include/file.hrl").


%% The behaviour of this service is inspired by the open source FAM
%% daemon [http://oss.sgi.com/projects/fam/].
%%
%% NOTE: Monitored paths should be absolute, but this is not checked.
%% 
%% We never change the paths, e.g. from relative to absolute, but we
%% make sure that the path is a flat string, for the sake of
%% comparisons, and return this to the caller for reference.


-define(DEFAULT_POLL_TIME, 5000).  % change with option poll_time
-define(MIN_POLL_TIME, 100).
-define(SERVER, ?MODULE).

-record(state, {poll_time,  % polling interval (milliseconds)
		dirs,       % map: directory path -> entry
		files,      % map: file path -> entry
		refs,       % map: file monitor Ref -> {Pid, Object}
		clients     % map: client Pid -> erlang:monitor/2 Ref
	       }).

-record(monitor, {pid, reference}).

-record(entry, {info = undefined, files = [], monitors = sets:new()}).


%%
%% User interface
%%

monitor_file(Path, Pid) ->
    monitor_file(?SERVER, Path, Pid).

monitor_file(Server, Path, Pid) ->
    monitor(Server, file, Path, Pid).


monitor_dir(Path, Pid) ->
    monitor_dir(?SERVER, Path, Pid).

monitor_dir(Server, Path, Pid) ->
    monitor(Server, dir, Path, Pid).


monitor(Server, Type, Path, Pid) when is_pid(Pid) ->
    Cmd = {monitor, {Type, filename:flatten(Path)}, Pid},
    {ok, Ref} = gen_server:call(Server, Cmd),
    {ok, Path, Ref}.


demonitor(Ref) ->
    demonitor(?SERVER, Ref).

demonitor(Server, Ref) ->
    ok = gen_server:call(Server, {demonitor, Ref}).


get_poll_time() ->
    get_poll_time(?SERVER).

get_poll_time(Server) ->
    gen_server:call(Server, get_poll_time).


set_poll_time(Time) ->
    set_poll_time(?SERVER, Time).

set_poll_time(Server, Time) ->
    gen_server:call(Server, {set_poll_time, Time}).
    


start() ->
    start([]).

start(Options) ->
    start({local, ?SERVER}, Options).

start(undefined, Options) ->
    gen_server:start(?MODULE, Options, []);
start(Name, Options) ->
    gen_server:start(Name, ?MODULE, Options, []).

start_link() ->
    start_link([]).

start_link(Options) ->
    start_link({local, ?SERVER}, Options).

start_link(undefined, Options) ->
    gen_server:start_link(?MODULE, Options, []);
start_link(Name, Options) ->
    gen_server:start_link(Name, ?MODULE, Options, []).


stop() ->
    stop(?SERVER).

stop(Server) ->
    gen_server:call(Server, stop),
    ok.


%%
%% gen_server callbacks
%%

init(Options) ->
    Time = safe_poll_time(proplists:get_value(poll_time, Options)),
    St = #state{poll_time = Time,
		dirs = dict:new(),
		files = dict:new(),
		clients = dict:new(),
		refs = dict:new()},
    set_timer(St),
    {ok, St}.


handle_call({monitor, Object, Pid}, _From, St) when is_pid(Pid) ->
    {Ref, St1} = new_monitor(Object, Pid, St),
    {reply, {ok, Ref}, add_client(Pid, St1)};
handle_call({demonitor, Ref}, _From, St) when is_reference(Ref) ->
    {reply, ok, delete_monitor(Ref, St)};
handle_call(get_poll_time, _From, St) ->
    {reply, St#state.poll_time, St};
handle_call({set_poll_time, Time}, _From, St) ->
    {reply, ok, St#state{poll_time=safe_poll_time(Time)}};
handle_call(stop, _From, St) ->
    {stop, normal, ok, St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info(poll_time, St) ->  
    {noreply, set_timer(poll(St))};
handle_info({'DOWN', _Ref, process, Pid, _Info}, St) ->
    {noreply, del_client(Pid, St)};
handle_info(_, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, _St) ->
    ok.

%%
%% Internal functions
%%

max(X, Y) when X > Y -> X;
max(_, Y) -> Y.

min(X, Y) when X < Y -> X;
min(_, Y) -> Y.

safe_poll_time(N) when is_integer(N) ->
    min(16#FFFFffff, max(N, ?MIN_POLL_TIME));
safe_poll_time(_) -> ?DEFAULT_POLL_TIME.

set_timer(St) ->
    erlang:send_after(St#state.poll_time, self(), poll_time),
    St.


%% client monitoring (once a client, always a client - until death)

add_client(Pid, St) ->
    case dict:is_key(Pid, St#state.clients) of
	true ->
	    St;
	false ->
	    Ref = erlang:monitor(process, Pid),
	    St#state{clients = dict:store(Pid, Ref, St#state.clients)}
    end.

del_client(Pid, St0) ->
    St = purge_pid(Pid, St0),
    case dict:find(Pid, St#state.clients) of
	{ok, Ref} ->
	    erlang:demonitor(Ref, [flush]),
	    St#state{clients = dict:erase(Pid, St#state.clients)};
	error ->
	    St
    end.


new_monitor(Object, Pid, St) ->
    Ref = make_ref(),
    Monitor = #monitor{pid = Pid, reference = Ref},
    new_monitor(Object, Monitor, Ref,
		St#state{refs = dict:store(Ref, {Pid, Object},
					   St#state.refs)}).

%% We must separate the namespaces for files and dirs, since we can't
%% trust the users to keep them distinct; there may be simultaneous file
%% and dir monitors for the same path (and a file may be deleted and
%% replaced by a directory of the same name, or vice versa).

new_monitor({file, Path}, Monitor, Ref, St) ->
    {Ref, St#state{files = add_monitor(Path, Monitor, file,
				       St#state.files)}};
new_monitor({dir, Path}, Monitor, Ref, St) ->
    {Ref, St#state{dirs = add_monitor(Path, Monitor, dir,
				      St#state.dirs)}}.

%% Adding a new monitor forces an immediate poll of the file, such that
%% previous monitors only see any real change, while the new monitor
%% either gets {exists, ...} or {error, ...}.

add_monitor(Path, Monitor, Type, Dict) ->
    Entry = case dict:find(Path, Dict) of
		{ok, OldEntry} -> poll_file(Path, OldEntry, Type);
		error -> new_entry(Path, Type)
	    end,
    event(#entry{}, dummy_entry(Entry, Monitor), Type, Path),
    NewEntry = Entry#entry{monitors =
			   sets:add_element(Monitor,
					    Entry#entry.monitors)},
    dict:store(Path, NewEntry, Dict).

dummy_entry(Entry, Monitor) ->
    Entry#entry{monitors = sets:add_element(Monitor, sets:new())}.

new_entry(Path, Type) ->
    refresh_entry(Path, #entry{monitors = sets:new()}, Type).

%% deleting a monitor by reference

delete_monitor(Ref, St) ->
    case dict:find(Ref, St#state.refs) of
	{ok, {_Pid, Object}} ->
	    St1 = St#state{refs = dict:erase(Ref, St#state.refs)},
	    delete_monitor_1(Ref, Object, St1);
	error ->
	    St
    end.

delete_monitor_1(Ref, {file, Path}, St) ->
    St#state{files = delete_monitor_2(Path, Ref, St#state.files)};
delete_monitor_1(Ref, {dir, Path}, St) ->
    St#state{dirs = delete_monitor_2(Path, Ref, St#state.dirs)}.

delete_monitor_2(Path, Ref, Dict) ->
    case dict:find(Path, Dict) of
	{ok, Entry} -> 
	    purge_empty_sets(
	      dict:store(Path, purge_monitor_ref(Ref, Entry), Dict));
	error ->
	    Dict
    end.

purge_monitor_ref(Ref, Entry) ->
    Entry#entry{monitors =
		sets:filter(fun (#monitor{reference = R})
				when R =:= Ref -> false;
				(_) -> true
			    end,
			    Entry#entry.monitors)}.

%% purging monitoring information related to a deleted client Pid

purge_pid(Pid, St) ->
    Files = dict:map(fun (_Path, Entry) ->
			     purge_monitor_pid(Pid, Entry)
		     end,
		     St#state.files),
    Dirs = dict:map(fun (_Path, Entry) ->
			    purge_monitor_pid(Pid, Entry)
		    end,
		    St#state.dirs),
    Refs = dict:filter(fun (_Ref, {P, _})
			   when P =:= Pid -> false;
			   (_, _) -> true
		       end,
		       St#state.refs),
    St#state{refs = Refs,
	     files = purge_empty_sets(Files),
	     dirs = purge_empty_sets(Dirs)}.

purge_monitor_pid(Pid, Entry) ->
    Entry#entry{monitors =
		sets:filter(fun (#monitor{pid = P})
				when P =:= Pid -> false;
				(_) -> true
			    end,
			    Entry#entry.monitors)}.

purge_empty_sets(Dict) ->
    dict:filter(fun (_Path, Entry) ->
			sets:size(Entry#entry.monitors) > 0
		end, Dict).


%% Generating events upon state changes by comparing old and new states
%% 
%% Message formats:
%%   {exists, Path, Type, #file_info{}, Files}
%%   {changed, Path, Type, #file_info{}, Files}
%%   {error, Path, Type, Info}
%%
%% Type is dir or file. If Type is file, Files is always []. If Type is
%% dir, Files is a list of {added, FileName} and {deleted, FileName},
%% where FileName is relative to dir, without any path component.
%%
%% When a new monitor is installed for a path, an initial {exists,...}
%% or {error,...} message will be sent to the monitor owner.
%%
%% Subsequent events will be either {changed,...} or {error,...}.

event(#entry{info = Info}, #entry{info = Info}, _Type, _Path) ->
    %% no change in state
    ok;
event(#entry{info = undefined}, #entry{info = NewInfo}=Entry,
      Type, Path)
  when not is_atom(NewInfo) ->
    %% file or directory exists, for a fresh monitor
    Files = [{added, F} || F <- Entry#entry.files],
    cast({exists, Path, Type, NewInfo, Files}, Entry#entry.monitors);
event(_OldEntry, #entry{info = NewInfo}=Entry, Type, Path)
  when is_atom(NewInfo) ->
    %% file is not available
    cast({error, Path, Type, NewInfo}, Entry#entry.monitors);
event(_OldEntry, #entry{info = Info}=Entry, file, Path) ->
    %% a normal file has changed or simply become accessible
    cast({changed, Path, file, Info, []}, Entry#entry.monitors);
event(#entry{info = OldInfo}, #entry{info = NewInfo}=Entry, dir, Path)
  when is_atom(OldInfo) ->
    %% a directory has become accessible
    Files = [{added, F} || F <- Entry#entry.files],
    cast({changed, Path, dir, NewInfo, Files}, Entry#entry.monitors);
event(OldEntry, #entry{info = Info}=Entry, dir, Path) ->
    %% a directory has changed
    Files = diff_lists(Entry#entry.files, OldEntry#entry.files),
    cast({changed, Path, dir, Info, Files}, Entry#entry.monitors).


poll(St) ->
    St#state{files = dict:map(fun (Path, Entry) ->
				      poll_file(Path, Entry, file)
			      end,
			      St#state.files),
	     dirs = dict:map(fun (Path, Entry) ->
				     poll_file(Path, Entry, dir)
			     end,
			     St#state.dirs)}.

poll_file(Path, Entry, Type) ->
    NewEntry = refresh_entry(Path, Entry, Type),
    event(Entry, NewEntry, Type, Path),
    NewEntry.

refresh_entry(Path, Entry, Type) ->
    Info = get_file_info(Path),
    Files = case Type of
		dir when not is_atom(Info) -> get_dir_files(Path);	    
		_ -> []
	    end,
    Entry#entry{info = Info, files = Files}.


%% We clear some fields of the file_info so that we only trigger on real
%% changes; see the //kernel/file.erl manual and file.hrl for details.

get_file_info(Path) ->
    case file:read_file_info(Path) of
	{ok, Info} ->
	    Info#file_info{access = undefined,
			   atime  = undefined};
	{error, Error} ->
	    Error  % posix error code as atom
    end.

%% Listing the members of a directory; note that it yields the empty
%% list if it fails - this is not the place for error detection.

get_dir_files(Path) ->
    case file:list_dir(Path) of
	{ok, Files} -> lists:sort(Files);
	{error, _} -> []
    end.

%% both lists must be sorted for this diff to work

diff_lists([F1 | Fs1], [F2 | _]=Fs2) when F1 < F2 ->
    [{added, F1} | diff_lists(Fs1, Fs2)];
diff_lists([F1 | _]=Fs1, [F2 | Fs2]) when F1 > F2 ->
    [{deleted, F2} | diff_lists(Fs1, Fs2)];
diff_lists([_ | Fs1], [_ | Fs2]) ->
    diff_lists(Fs1, Fs2);
diff_lists([F | Fs1], Fs2) ->
    [{added, F} | diff_lists(Fs1, Fs2)];
diff_lists(Fs1, [F | Fs2]) ->
    [{deleted, F} | diff_lists(Fs1, Fs2)];
diff_lists([], []) ->
    [].


%% Multicasting events to clients

cast(Msg, Monitors) ->
    sets:fold(fun (#monitor{pid = Pid, reference = Ref}, Msg) ->
		      %%erlang:display({file_monitor, Ref, Msg}),
		      Pid ! {file_monitor, Ref, Msg},
		      Msg  % note that this is a fold, not a map
	      end,
	      Msg, Monitors).

%% test() ->
%%     Pid = spawn(fun loop/0),
%%     register(fred,Pid).

%% loop() ->
%%     receive X -> erlang:display(X) end,
%%     loop().
