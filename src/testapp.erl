-module(testapp).

-export([insertKeys/1,
    get/1,
    insertAndUpdate/2,
    iteratorAdvance/1,
    fold_example/0,
    order_dict_example/0,
    test_double_open/0,
    concurrent_read/0,
    read_randomly_n_times/1,
    ets_concurrent_reads/0,
    insert_n_keys_ets/1,
    read_sec_ets/1,
    check_iterator_order_complex_key/0,
    check_iterator_order/0]).

%% Insert Number keys (0, Number] with the same value
insertKeys(Number) ->
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    insert_unique_keys_rec(Ref, Number, Number),
    eleveldb:close(Ref).

insertAndUpdate(InsertNumber, UpdateNumber) ->
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    insert_unique_keys_rec_lists(Ref, InsertNumber, InsertNumber),
    ok = update_keys(Ref, InsertNumber, UpdateNumber),
    eleveldb:close(Ref).

update_keys(_Ref, _InsertNumber, 0) ->
    ok;
update_keys(Ref, InsertNumber, UpdateNumber) ->
    Rand = random:uniform(InsertNumber) - 1,
    BinaryRef = integer_to_binary(Rand),
    {ok, Res} = eleveldb:get(Ref, BinaryRef, []),
    List = [integer_to_binary(UpdateNumber) | Res],
    eleveldb:put(Ref, BinaryRef, list_to_binary(List), []),
    update_keys(Ref, InsertNumber, UpdateNumber - 1).

%% Inserts unique keys with lists as values
insert_unique_keys_rec_lists(_Ref, _N, 0) ->
    ok;
insert_unique_keys_rec_lists(Ref, N, A) ->
    BinaryRef = integer_to_binary(N - A),
    EmptyList = list_to_binary([]),
    eleveldb:put(Ref, BinaryRef, EmptyList, []),
    insert_unique_keys_rec(Ref, N, A - 1).

%% Inserts unique keys with numbers as values
insert_unique_keys_rec(_Ref, _N, 0) ->
    ok;
insert_unique_keys_rec(Ref, N, A) ->
    BinaryRef = integer_to_binary(N - A),
    eleveldb:put(Ref, BinaryRef, BinaryRef, []),
    insert_unique_keys_rec(Ref, N, A - 1).

%% simple get
get(Number) ->
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    BinaryRef = integer_to_binary(Number),
    {ok, Res} = eleveldb:get(Ref, BinaryRef, []),
    eleveldb:close(Ref),
    Res.

%% check operations in iterator class
iteratorAdvance(_TimesToAdvance) ->
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    {ok, ItRef} = eleveldb:iterator(Ref, [], keys_only),
    {ok, Key1} = eleveldb:iterator_move(ItRef, first),
    {ok, Key2} = eleveldb:iterator_move(ItRef, next),
    {ok, Key3} = eleveldb:iterator_move(ItRef, prefetch),
    {Key1, Key2, Key3}.

%% Checks how fold works
fold_example() ->
    insertKeys(10),
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    print_DB(Ref),
    eleveldb:close(Ref).

%% Checks how does a compound key behave
order_dict_example() ->
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    eleveldb:put(Ref, term_to_binary({[1, 0, 1], a}), integer_to_binary(1), []),
    eleveldb:put(Ref, term_to_binary({[1, 1, 1], b}), integer_to_binary(2), []),
    eleveldb:put(Ref, term_to_binary({[2, 0, 1], a}), integer_to_binary(3), []),
    eleveldb:put(Ref, term_to_binary({[1, 0, 0], c}), integer_to_binary(4), []),
    eleveldb:put(Ref, term_to_binary({[5, 0, 1], a}), integer_to_binary(5), []),
    eleveldb:put(Ref, term_to_binary({[3, 0, 4], a}), integer_to_binary(6), []),
    print_DB(Ref),
    eleveldb:close(Ref).

%% Prints the DB in Ref
print_DB(Ref) ->
    eleveldb:fold(
        Ref,
        fun(A, AccIn) -> io:format("~p ", [A]), AccIn end,
        [],
        []).

%% Opening the same DB twice should crash
test_double_open() ->
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    {ok, Ref1} = eleveldb:open("testDB", [{create_if_missing, true}]),
    eleveldb:close(Ref),
    eleveldb:close(Ref1).


%% create 4 threads each reading 100000 times 25% of the key space
concurrent_read() ->
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    spawn(testapp, read_randomly_n_times_ref, [Ref, 100000, self(), 0]),
    spawn(testapp, read_randomly_n_times_ref, [Ref, 100000, self(), 2500]),
    spawn(testapp, read_randomly_n_times_ref, [Ref, 100000, self(), 5000]),
    spawn(testapp, read_randomly_n_times_ref, [Ref, 100000, self(), 7500]),
    wait_for_termination(4),
    eleveldb:close(Ref).

read_randomly_n_times_ref(_Ref, 0, ParentRef, _Off) ->
    ParentRef ! finished;
read_randomly_n_times_ref(Ref, N, ParentRef, Off) ->
    Rand = Off + random:uniform(2500),
    Binary = integer_to_binary(Rand),
    eleveldb:get(Ref, Binary, []),
    read_randomly_n_times_ref(Ref, N - 1, ParentRef, Off).

wait_for_termination(0) ->
    ok;
wait_for_termination(Threads) ->
    receive
        finished ->
            wait_for_termination(Threads - 1)
    end.

%% Reads randomly N times from the keys in the DB
read_randomly_n_times(N) ->
    {ok, Ref} = eleveldb:open("testDB", [{create_if_missing, true}]),
    ok = read_rec(Ref, N),
    eleveldb:close(Ref).

read_rec(_Ref, 0) ->
    ok;
read_rec(Ref, N) ->
    Rand = random:uniform(10000),
    Binary = integer_to_binary(Rand),
    eleveldb:get(Ref, Binary, []),
    read_rec(Ref, N - 1).


%%%%%%%% Methods to compare ETS to levelDB
insert_n_keys_ets(N) ->
    ets:new(test, [set, protected, named_table, {read_concurrency, true}]),
    insert_rec_ets(test, N).

insert_rec_ets(_Name, 0) ->
    ok;
insert_rec_ets(Name, N) ->
    ets:insert(Name, {pepe, N}),
    insert_rec_ets(Name, N - 1).

ets_concurrent_reads() ->
    spawn(testapp, read_randomly_n_times_ets, [test, 100000, self(), 0]),
    spawn(testapp, read_randomly_n_times_ets, [test, 100000, self(), 2500]),
    spawn(testapp, read_randomly_n_times_ets, [test, 100000, self(), 5000]),
    spawn(testapp, read_randomly_n_times_ets, [test, 100000, self(), 7500]),
    wait_for_termination(4).

read_randomly_n_times_ets(_Name, 0, ParentRef, _Off) ->
    ParentRef ! finished;
read_randomly_n_times_ets(Name, N, ParentRef, Off) ->
    Rand = Off + random:uniform(2500),
    ets:lookup(Name, Rand),
    read_randomly_n_times_ets(Name, N - 1, ParentRef, Off).

read_sec_ets(0) ->
    ok;
read_sec_ets(N) ->
    Rand = random:uniform(10000),
    ets:lookup(Rand, Rand),
    read_sec_ets(N - 1).

%%%%%%%%


check_iterator_order() ->
    {ok, Ref} = eleveldb:open("check_iterator_order_DB", [{create_if_missing, true}]),
    eleveldb:put(Ref, term_to_binary({1, 1}), term_to_binary(1), []),
    eleveldb:put(Ref, term_to_binary({1, 2}), term_to_binary(1), []),
    eleveldb:put(Ref, term_to_binary({2, 1}), term_to_binary(2), []),
    eleveldb:put(Ref, term_to_binary({3, 1}), term_to_binary(3), []),
    eleveldb:put(Ref, term_to_binary({4, 1}), term_to_binary(4), []),
    eleveldb:put(Ref, term_to_binary({5, 1}), term_to_binary(5), []),
    eleveldb:put(Ref, term_to_binary({2, 2}), term_to_binary(2), []),
    eleveldb:put(Ref, term_to_binary({3, 4}), term_to_binary(3), []),
    eleveldb:put(Ref, term_to_binary({1, 4}), term_to_binary(1), []),
    eleveldb:put(Ref, term_to_binary({2, 3}), term_to_binary(2), []),

    %% iterate the 3 keys that start with two
    try
        eleveldb:fold(Ref,
            fun({K, V}, AccIn) ->
                case AccIn of
                    0 ->
                        throw({break, AccIn});
                    _ ->
                        io:format("~p : ~p ~n", [binary_to_term(K), binary_to_term(V)]),
                        AccIn - 1
                end
            end,
            3,
            [{first_key, term_to_binary({2, 1})}])
    catch
        {break, 0} ->
            ok
    end,
    eleveldb:close(Ref),
    eleveldb:destroy("check_iterator_order_DB", []).


check_iterator_order_complex_key() ->
    {ok, Ref} = eleveldb:open("check_iterator_order_complex_key_DB", [{create_if_missing, true}]),
    eleveldb:put(Ref, term_to_binary({1, {1, [1, 2, 3]}}), term_to_binary(1), []),
    eleveldb:put(Ref, term_to_binary({1, {2, [1, 2, 3]}}), term_to_binary(1), []),
    eleveldb:put(Ref, term_to_binary({2, {1, [1, 3, 3]}}), term_to_binary(2), []),
    eleveldb:put(Ref, term_to_binary({3, {1, [1, 2, 3]}}), term_to_binary(3), []),
    eleveldb:put(Ref, term_to_binary({4, {1, [1, 2, 3]}}), term_to_binary(4), []),
    eleveldb:put(Ref, term_to_binary({5, {1, [1, 2, 3]}}), term_to_binary(5), []),
    eleveldb:put(Ref, term_to_binary({2, {2, [4, 2, 3]}}), term_to_binary(2), []),
    eleveldb:put(Ref, term_to_binary({3, {4, [1, 2, 3]}}), term_to_binary(3), []),
    eleveldb:put(Ref, term_to_binary({1, {4, [1, 2, 3]}}), term_to_binary(1), []),
    eleveldb:put(Ref, term_to_binary({2, {3, [1, 9, 3]}}), term_to_binary(2), []),

    %% iterate the 3 keys that start with two
    try
        eleveldb:fold(Ref,
            fun({K, V}, AccIn) ->
                case AccIn of
                    0 ->
                        throw({break, AccIn});
                    _ ->
                        io:format("~p : ~p ~n", [binary_to_term(K), binary_to_term(V)]),
                        AccIn - 1
                end
            end,
            3,
            [{first_key, term_to_binary({2, 1})}])
    catch
        {break, 0} ->
            ok
    end,
    eleveldb:close(Ref),
    eleveldb:destroy("check_iterator_order_complex_key_DB", []).
