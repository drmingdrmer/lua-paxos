
function tuple()
    return 1, 2, 3
end

function tb()
    return {1, 2, 3}
end

function _ben(f, times)
    local t0 = os.time()
    for i = 1, times do
        f()
    end
    local t1 = os.time()

    local spent = t1-t0
    print(times, spent)
    return spent
end

function ben(f)
    local times = 100
    local spent = 0
    while spent < 10 do
        times = times * 2
        spent = _ben(f, times)
    end

    print( "spent:", spent, "times:", times, "rps:", math.floor(times/spent) )
end

ben(tuple)
ben(tb)

