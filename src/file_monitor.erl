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

-export([monitor_file/1, monitor_file/2, monitor_file/3, monitor_dir/1,
	 monitor_dir/2, monitor_dir/3, automonitor/1, automonitor/2,
	 automonitor/3, demonitor/1, demonitor/2, get_poll_time/0,
	 get_poll_time/1, set_poll_time/1, set_poll_time/2]).

-export([start/0, start/1, start/2, start_link/0, start_link/1,
	 start_link/2, stop/0, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 code_change/3, terminate/2]).

-include_lib("kernel/include/file.hrl").
%%-include_lib("eunit/include/eunit.hrl").
-include("../include/eunit.hrl").


%% The behaviour of this service is inspired by the open source FAM
%% daemon [http://oss.sgi.com/projects/fam/].
%%
%% NOTE: Monitored paths should be absolute, but this is not checked.
%% 
%% We never rewrite the paths, e.g. from relative to absolute, but we
%% ensure that every path is a flat string internally, for the sake of
%% comparisons, and return it to the caller for reference.
%% TODO: store paths internally as UTF8-encoded binaries

-define(DEFAULT_POLL_TIME, 5000).  % change with option poll_time
-define(MIN_POLL_TIME, 100).
-define(SERVER, ?MODULE).
-define(MSGTAG, ?SERVER).

%% @type object() -> {file|directory, filename()}
%% @type monitor() -> {pid(), reference()}

-record(state, {poll_time,  % polling interval (milliseconds)
		dirs,       % map: directory path -> #entry{}
		files,      % map: file path -> #entry{}
		refs,       % map: monitor Ref -> #monitor_info{}
		clients     % map: client Pid -> #client_info{}
	       }).

-record(entry, {info = undefined,      % #file_info{} or posix atom
		files = [],            % directory entries (if any)
		monitors = sets:new()  % set(monitor())
	       }).

-record(client_info, {monitor,    % erlang:monitor/2 reference
		      refs        % file monitor references owned by client
		     }).

-record(monitor_info, {pid,     % client Pid
		       objects  % set(object())
		      }).

%%
%% User interface
%%

monitor_file(Path) ->
    monitor_file(Path, []).

monitor_file(Path, Opts) ->
    monitor_file(?SERVER, Path, Opts).

monitor_file(Server, Path, Opts) ->
    monitor(Server, Path, Opts, file).


monitor_dir(Path) ->
    monitor_dir(Path, []).

monitor_dir(Path, Opts) ->
    monitor_dir(?SERVER, Path, Opts).

monitor_dir(Server, Path, Opts) ->
    monitor(Server, Path, Opts, directory).


%% not exported
monitor(Server, Path, Opts, Type) ->
    Pid = self(),  %% the Pid of the calling client
    FlatPath = filename:flatten(Path),
    Ref = case proplists:get_value(reference, Opts) of
	      R when is_reference(R) -> R;
	      undefined -> make_ref();
	      _ -> erlang:error(badarg)
	  end,
    Cmd = {monitor, {Type, FlatPath}, Pid, Ref},
    case gen_server:call(Server, Cmd) of
	ok -> {ok, Ref, FlatPath};
	{error, Reason} -> {error, Reason}
    end.


automonitor(Path) ->
    automonitor(Path, []).

automonitor(Path, Opts) ->
    automonitor(?SERVER, Path, Opts).

automonitor(Server, Path, _Opts) ->
    Pid = self(),  %% the Pid of the calling client
    FlatPath = filename:flatten(Path),
    {ok, Ref} = gen_server:call(Server, {automonitor, Path, Pid}),
    {ok, FlatPath, Ref}.


demonitor(Ref) ->
    demonitor(?SERVER, Ref).

demonitor(Server, Ref) when is_reference(Ref) ->
    ok = gen_server:call(Server, {demonitor, Ref}).


get_poll_time() ->
    get_poll_time(?SERVER).

get_poll_time(Server) ->
    gen_server:call(Server, get_poll_time).


set_poll_time(Time) ->
    set_poll_time(?SERVER, Time).

set_poll_time(Server, Time) when is_integer(Time) ->
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


handle_call({monitor, Object, Pid, Ref}, _From, St)
  when is_pid(Pid), is_reference(Ref) ->
    try add_monitor(Object, Pid, Ref, St) of
	St1 -> {reply, ok, register_client(Pid, Ref, St1)}
    catch
	not_owner ->
	    {reply, {error, not_owner}, St}
    end;
handle_call({demonitor, Ref}, _From, St) when is_reference(Ref) ->
    {reply, ok, delete_monitor(Ref, St)};
handle_call({automonitor, Path, Pid}, _From, St) when is_pid(Pid) ->
    Ref = make_ref(),
    St1 = automonitor(Path, Pid, Ref, St),
    {reply, {ok, Ref}, register_client(Pid, Ref, St1)};
handle_call(get_poll_time, _From, St) ->
    {reply, St#state.poll_time, St};
handle_call({set_poll_time, Time}, _From, St) ->
    {reply, ok, St#state{poll_time=safe_poll_time(Time)}};
handle_call(stop, _From, St) ->
    {stop, normal, ok, St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info({?MSGTAG, Ref, Event}, St) ->
    %% auto-monitoring event to self
    case dict:find(Ref, St#state.refs) of
	{ok, #monitor_info{pid=Pid}} ->
	    {noreply, autoevent(Event, Pid, Ref, St)};
	error ->
	    %% could happen if this event was already in the queue when
	    %% we processed the deletion of the same reference, so just
	    %% ignore the message
	    {noreply, St}
    end;
handle_info(poll_time, St) ->
    {noreply, set_timer(poll(St))};
handle_info({'DOWN', _Ref, process, Pid, _Info}, St) ->
    {noreply, remove_client(Pid, St)};
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

%% handle auto-monitoring events

autoevent({exists, Path, directory, #file_info{}=Info, Files}, Pid, Ref, St)
  when Info#file_info.type =:= directory ->
    %% add automonitoring to all directory entries
    lists:foldl(fun ({added, File}) ->
			automonitor(filename:join(Path, File), Pid, Ref, St)
		end,
		Files, St);
autoevent({changed, Path, directory, #file_info{}=Info, Files}, Pid, Ref, St)
  when Info#file_info.type =:= directory ->
    %% add/remove automonitoring to/from all added/deleted entries
    lists:foldl(fun ({added, File}) ->
			automonitor(filename:join(Path, File), Pid, Ref, St);
		    ({deleted, File}) ->
			autodemonitor(filename:join(Path, File), Pid, Ref, St)
		end,
		Files, St);
autoevent(_Event, _Pid, _Ref, St) ->
    %% TODO: other events that need handling?
    St.

%% - if a new entry of an automonitored directory is discovered, make it
%%   too automonitored (this does recursive monitoring by definition)
%%
%% - if an entry is deleted from an automonitored directory, remove any
%%   automonitor from it and its subdirectories recursively (note that
%%   the top level directory cannot be removed this way, by definition)
%%
%% - errors on automonitored files are assumed to be intermittent
%%
%% - an automonitored non-directory that changes type to directory
%%   should cause recursive monitoring (changing monitor type from file
%%   to directory)
%%
%% - an automonitored directory that changes type to non-directory
%%   should cause recursive demonitoring of any subdirectories (changing
%%   monitor type from directory to file)
%%
%% - not that when a demonitoring an ex-directory, we cannot list it to
%%   find its subdirectory paths - we must check path prefixes!

automonitor(Path, Pid, Ref, St) ->
    %% Pid should be a known client already, otherwise ignore this
    case dict:is_key(Pid, St#state.clients) of
	true ->
	    case file:read_file_info(Path) of
		{ok, #file_info{type=directory}} ->
		    Object = {directory, Path},
		    St1 = add_monitor(Object, Pid, Ref, St),
		    add_automonitor(Object, Ref, St1);
		_ ->
		    add_monitor({file, Path}, Pid, Ref, St)
	    end;
	false ->
	    St
    end.

add_automonitor(_Object, _Ref, St) ->
    %% the Object is always a directory here
    %% FIXME:
%%%     Objects = case dict:find(Ref, St#state.refs) of
%%% 		  {ok, #monitor_info{objects = Set}} -> Set;
%%% 		  error -> sets:new()
%%% 	      end,
%%%     Objects1 = sets:add_element(Object, Objects),
%%%     Refs = dict:store(Ref, #monitor_info{pid = Pid, objects = Objects1},
%%% 		          St#state.refs),
%%%     Monitor = {Pid, Ref},
%%%     monitor_path(Object, Monitor, St#state{refs = Refs}).
    St.

autodemonitor(_Path, _Pid, _Ref, St) ->
    %% FIXME:
    St.


%% client monitoring (once a client, always a client - until death)

register_client(Pid, Ref, St) ->
    Info = case dict:find(Pid, St#state.clients) of
	       {ok, OldInfo} -> OldInfo;
	       error ->
		   Monitor = erlang:monitor(process, Pid),
		   #client_info{monitor=Monitor, refs = sets:new()}
	   end,
    Refs = sets:add_element(Ref, Info#client_info.refs),
    St#state{clients = dict:store(Pid, Info#client_info{refs = Refs},
				  St#state.clients)}.

remove_client(Pid, St0) ->
    St = purge_pid(Pid, St0),
    case dict:find(Pid, St#state.clients) of
	{ok, #client_info{monitor = Ref}} ->
	    erlang:demonitor(Ref, [flush]),
	    St#state{clients = dict:erase(Pid, St#state.clients)};
	error ->
	    St
    end.

%% Adding a new monitor; throws not_owner if the monitor reference is
%% already registered for another Pid.

add_monitor(Object, Pid, Ref, St) ->
    Info = case dict:find(Ref, St#state.refs) of
	       {ok, #monitor_info{pid = Pid}=OldInfo} -> OldInfo;
	       {ok, #monitor_info{}} -> throw(not_owner);
	       error -> #monitor_info{pid = Pid, objects = sets:new()}
	   end,
    NewObjects = sets:add_element(Object, Info#monitor_info.objects),
    Refs = dict:store(Ref, Info#monitor_info{objects = NewObjects},
		      St#state.refs),
    monitor_path(Object, {Pid, Ref}, St#state{refs = Refs}).

%% We must separate the namespaces for files and dirs; there may be
%% simultaneous file and directory monitors for the same path, and a
%% file may be deleted and replaced by a directory of the same name, or
%% vice versa. The client should know (more or less) if a path is
%% expected to refer to a file or a directory.

monitor_path({file, Path}, Monitor, St) ->
    St#state{files = monitor_path(Path, Monitor, file, St#state.files)};
monitor_path({directory, Path}, Monitor, St) ->
    St#state{dirs = monitor_path(Path, Monitor, directory, St#state.dirs)}.

%% Adding a new monitor forces an immediate poll of the path, such that
%% previous monitors only see any real change, while the new monitor
%% either gets {exists, ...} or {error, ...}.

monitor_path(Path, Monitor, Type, Dict) ->
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
	{ok, #monitor_info{objects = Objects}} ->
	    sets:fold(fun (Object, St0) ->
			      demonitor_path(Ref, Object, St0)
		      end,
		      St#state{refs = dict:erase(Ref, St#state.refs)},
		      Objects);
	error ->
	    St
    end.

demonitor_path(Ref, {file, Path}, St) ->
    St#state{files = demonitor_path_1(Path, Ref, St#state.files)};
demonitor_path(Ref, {directory, Path}, St) ->
    St#state{dirs = demonitor_path_1(Path, Ref, St#state.dirs)}.

demonitor_path_1(Path, Ref, Dict) ->
    case dict:find(Path, Dict) of
	{ok, Entry} -> 
	    purge_empty_sets(
	      dict:store(Path, purge_monitor_ref(Ref, Entry), Dict));
	error ->
	    Dict
    end.

purge_monitor_ref(Ref, Entry) ->
    Entry#entry{monitors =
		sets:filter(fun ({_P, R})
				when R =:= Ref -> false;
				(_) -> true
			    end,
			    Entry#entry.monitors)}.

%% purging monitoring information related to a deleted client Pid
%% TODO: this should use the refs mapping to avoid visiting all paths

purge_pid(Pid, St) ->
    Files = dict:map(fun (_Path, Entry) ->
			     purge_monitor_pid(Pid, Entry)
		     end,
		     St#state.files),
    Dirs = dict:map(fun (_Path, Entry) ->
			    purge_monitor_pid(Pid, Entry)
		    end,
		    St#state.dirs),
    Refs = dict:filter(fun (_Ref, #monitor_info{pid = P})
			   when P =:= Pid -> false;
			   (_, _) -> true
		       end,
		       St#state.refs),
    St#state{refs = Refs,
	     files = purge_empty_sets(Files),
	     dirs = purge_empty_sets(Dirs)}.

purge_monitor_pid(Pid, Entry) ->
    Entry#entry{monitors =
		sets:filter(fun ({P, _R})
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
%% Type is file or directory, as specified by the monitor type, not by
%% the actual type on disk. If Type is file, Files is always []. If Type
%% is directory, Files is a list of {added, FileName} and {deleted,
%% FileName}, where FileName is on basename form, i.e., without any
%% directory component.
%%
%% When a new monitor is installed for a path, an initial {exists,...}
%% or {error,...} message will be sent to the monitor owner.
%%
%% Subsequent events will be either {changed,...} or {error,...}.
%%
%% The monitor reference is not included in the event descriptor itself,
%% but is part of the main message format; see cast/2.

%% TODO: report changes of file type as a delete plus an add??
%% (note that we don't see type changes of unmonitored directory entries)
%% perhaps report as an enoent followed by a change? is this useful??

event(#entry{info = Info}, #entry{info = Info}, _Type, _Path) ->
    %% no change in state (note that we never compare directory entry
    %% lists here; if there have been changes, the timestamps should
    %% also be different - see list_dir/2 below)
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
event(#entry{info = OldInfo}, #entry{info = NewInfo}=Entry, directory, Path)
  when is_atom(OldInfo) ->
    %% a directory has become accessible
    Files = [{added, F} || F <- Entry#entry.files],
    cast({changed, Path, directory, NewInfo, Files}, Entry#entry.monitors);
event(OldEntry, #entry{info = Info}=Entry, directory, Path) ->
    %% a directory has changed
    Files = diff_lists(Entry#entry.files, OldEntry#entry.files),
    cast({changed, Path, directory, Info, Files}, Entry#entry.monitors).


poll(St) ->
    St#state{files = dict:map(fun (Path, Entry) ->
				      poll_file(Path, Entry, file)
			      end,
			      St#state.files),
	     dirs = dict:map(fun (Path, Entry) ->
				     poll_file(Path, Entry, directory)
			     end,
			     St#state.dirs)}.

poll_file(Path, Entry, Type) ->
    NewEntry = refresh_entry(Path, Entry, Type),
    event(Entry, NewEntry, Type, Path),
    NewEntry.

refresh_entry(Path, Entry, Type) ->
    OldInfo = Entry#entry.info,
    Info = get_file_info(Path),
    case Type of
	directory when not is_atom(Info) ->
	    case Info#file_info.type of
		directory when is_atom(OldInfo)
		; Info#file_info.mtime =/= OldInfo#file_info.mtime ->
		    %% we only list directories when they have new
		    %% timestamps, to keep the polling cost down
		    Files = list_dir(Path, Info#file_info.mtime),
		    Entry#entry{info = Info, files = Files};
		directory ->
		    Entry#entry{info = Info};
		_ ->
		    %% attempting to monitor a non-directory as a
		    %% directory is reported as an 'enotdir' error
		    Entry#entry{info = enotdir, files = []}
	    end;
	_ ->
	    %% if we're not monitoring this path as a directory, we
	    %% don't care what it is exactly, but just track its status
	    Entry#entry{info = Info}
    end.

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

list_dir(Path, Mtime) ->
    Files = case file:list_dir(Path) of
		{ok, Fs} -> Fs;
		{error, _} -> []
	    end,
    %% The directory access time should now be the time when we read the
    %% listing (at least), i.e., the current time.
    case file:read_file_info(Path) of
	{ok, #file_info{atime = Atime}} when Mtime =:= Atime ->
	    %% Recall that the timestamps have whole-second resolution.
	    %% If we listed the directory during the same second that it
	    %% was (or is still being) modified, it is possible that we
	    %% did not see all the additions/deletions that were made
	    %% that second - and in that case, we will not report those
	    %% changes until the timestamp changes again (maybe never).
	    %% To avoid that, we pause and list the directory again.
	    %% This of course affects the latency of this server in
	    %% certain situations, so a better solution would be to
	    %% abort the current refresh for this directory and schedule
	    %% it for a new asynchronous refresh after a small delay
	    %% (unless the poll interval is already below 0.5 seconds).
	    receive after 550 -> list_dir(Path, Mtime) end;
	_ ->
	    %% ignore possible errors when reading file_info as well
	    lists:sort(Files)
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


%% Multicasting events to clients. The message has the form
%% {file_monitor, MonitorReference, Event}, where Event is described in
%% more detail above, and 'file_monitor' is the name of this module.

cast(Msg, Monitors) ->
    sets:fold(fun ({Pid, Ref}, Msg) ->
		      Pid ! {?MSGTAG, Ref, Msg},
		      Msg  % note that this is a fold, not a map
	      end,
	      Msg, Monitors).
